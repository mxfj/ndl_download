#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NDL《山海經18卷》数字归档器 V5

目标：
    永久归档 NDL 数字化古籍页面

当前资料：
    Root PID : 2606132
    Volume 1 : 2555513 / 63 pages
    Volume 2 : 2555514 / 85 pages
    Volume 3 : 2555515 / 49 pages
    Volume 4 : 2555516 / 48 pages
    Total    : 245 pages

图片接口：
    https://dl.ndl.go.jp/api/iiif/{PID}/R{page:07d}/full/full/0/default.jpg

功能：
    1. 自动创建归档目录
    2. 保存 NDL 元数据
    3. 保存 TOC
    4. SQLite 页面数据库
    5. IIIF 原图下载
    6. .part 临时文件
    7. 断点续传
    8. 自动重试
    9. SHA256
    10. 缺页检测
    11. 下载状态管理
    12. SHA256SUMS
    13. HTML 报告
    14. PDF 合并
"""

import argparse
import concurrent.futures
import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

import requests


# ============================================================
# 基础配置
# ============================================================

ROOT_PID = "2606132"

BASE_DIR = Path(__file__).resolve().parent

ARCHIVE_DIR = BASE_DIR / "山海经_明刊古籍数字典藏"

RAW_DIR = ARCHIVE_DIR / "00_原始资料"
IMAGE_DIR = ARCHIVE_DIR / "01_原始影像"
CHECKSUM_DIR = ARCHIVE_DIR / "02_校验"
OCR_DIR = ARCHIVE_DIR / "03_OCR"
PDF_DIR = ARCHIVE_DIR / "04_PDF"
PICTURE_DIR = ARCHIVE_DIR / "05_图片"
RESEARCH_DIR = ARCHIVE_DIR / "06_研究资料"
DOC_DIR = ARCHIVE_DIR / "99_档案说明"

DB_FILE = ARCHIVE_DIR / "archive.db"
LOG_FILE = ARCHIVE_DIR / "archive.log"

ROOT_META_FILE = RAW_DIR / "root_2606132.json"
TOC_FILE = RAW_DIR / "toc_2606132.json"

SHA_FILE = CHECKSUM_DIR / "SHA256SUMS.txt"
REPORT_FILE = ARCHIVE_DIR / "archive_report.html"

# NDL
NDL_BASE = "https://dl.ndl.go.jp"
IIIF_BASE = "https://dl.ndl.go.jp/api/iiif"

TOC_URL = (
    "https://dl.ndl.go.jp/api/meta/search/toc/facet/"
    + ROOT_PID
)

# 已经从 NDL TOC 确认
VOLUMES = [
    {
        "volume_no": 1,
        "pid": "2555513",
        "title": "第一册",
        "pages": 63,
    },
    {
        "volume_no": 2,
        "pid": "2555514",
        "title": "第二册",
        "pages": 85,
    },
    {
        "volume_no": 3,
        "pid": "2555515",
        "title": "第三册",
        "pages": 49,
    },
    {
        "volume_no": 4,
        "pid": "2555516",
        "title": "第四册",
        "pages": 48,
    },
]

TOTAL_PAGES = sum(v["pages"] for v in VOLUMES)

DEFAULT_TIMEOUT = 120

# 每个页面最多尝试次数
DEFAULT_RETRIES = 5

# 下载块
CHUNK_SIZE = 1024 * 1024

# 默认并发
DEFAULT_WORKERS = 4


# ============================================================
# 日志
# ============================================================

def setup_logging():
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(
                LOG_FILE,
                encoding="utf-8"
            ),
            logging.StreamHandler(sys.stdout)
        ]
    )


logger = logging.getLogger("ndl")


# ============================================================
# 工具
# ============================================================

def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_dirs():
    dirs = [
        ARCHIVE_DIR,
        RAW_DIR,
        IMAGE_DIR,
        CHECKSUM_DIR,
        OCR_DIR / "raw",
        OCR_DIR / "corrected",
        OCR_DIR / "structured",
        PDF_DIR,
        PICTURE_DIR,
        RESEARCH_DIR,
        DOC_DIR,
    ]

    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def atomic_write_json(path: Path, data):
    tmp = path.with_suffix(path.suffix + ".part")

    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2
        )

    tmp.replace(path)


def sha256_file(path: Path):
    h = hashlib.sha256()

    with open(path, "rb") as f:
        while True:
            chunk = f.read(CHUNK_SIZE)

            if not chunk:
                break

            h.update(chunk)

    return h.hexdigest()


def get_session():
    session = requests.Session()

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "Chrome/140 Safari/537.36 "
            "NDL-Archive/5.0"
        ),
        "Accept": "*/*",
        "Connection": "keep-alive",
    })

    return session


# ============================================================
# SQLite
# ============================================================

def init_db():
    conn = sqlite3.connect(DB_FILE)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,

        volume_no INTEGER NOT NULL,
        volume_pid TEXT NOT NULL,

        page_no INTEGER NOT NULL,

        ndl_resource_id TEXT NOT NULL,

        original_filename TEXT,
        ndl_filename TEXT,

        download_url TEXT NOT NULL,

        local_path TEXT NOT NULL,

        file_size INTEGER DEFAULT 0,

        sha256 TEXT,

        status TEXT DEFAULT 'pending',

        http_status INTEGER,

        retry_count INTEGER DEFAULT 0,

        error_message TEXT,

        download_time TEXT,

        verify_time TEXT,

        UNIQUE(volume_pid, page_no)
    )
    """)

    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_pages_status
    ON pages(status)
    """)

    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_pages_volume
    ON pages(volume_pid)
    """)

    conn.commit()

    return conn


def upsert_page(
    conn,
    volume_no,
    volume_pid,
    page_no,
    resource_id,
    filename,
    url,
    local_path
):

    conn.execute("""
    INSERT INTO pages (
        volume_no,
        volume_pid,
        page_no,
        ndl_resource_id,
        original_filename,
        ndl_filename,
        download_url,
        local_path
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)

    ON CONFLICT(volume_pid, page_no)
    DO UPDATE SET
        ndl_resource_id=excluded.ndl_resource_id,
        original_filename=excluded.original_filename,
        ndl_filename=excluded.ndl_filename,
        download_url=excluded.download_url,
        local_path=excluded.local_path
    """, (
        volume_no,
        volume_pid,
        page_no,
        resource_id,
        filename,
        filename,
        url,
        local_path,
    ))

    conn.commit()


# ============================================================
# NDL metadata
# ============================================================

def download_json(url, output):
    session = get_session()

    logger.info("GET %s", url)

    try:
        r = session.get(
            url,
            timeout=DEFAULT_TIMEOUT
        )

        logger.info(
            "HTTP %s | %s",
            r.status_code,
            url
        )

        r.raise_for_status()

        data = r.json()

        atomic_write_json(
            output,
            data
        )

        return data

    except Exception as e:
        logger.error(
            "获取 JSON 失败: %s",
            e
        )

        return None


def get_root_metadata():
    """
    尝试获取当前 NDL 元数据。

    注意：
    旧版 dl.ndl.go.jp/api/meta/{PID}
    并不是当前有效接口。

    因此这里主要保存我们已经确认的 TOC。
    """

    if TOC_FILE.exists():
        try:
            with open(
                TOC_FILE,
                "r",
                encoding="utf-8"
            ) as f:
                return json.load(f)
        except Exception:
            pass

    return None


def download_toc():
    if TOC_FILE.exists():
        logger.info(
            "TOC 已存在，跳过下载：%s",
            TOC_FILE
        )

        try:
            with open(
                TOC_FILE,
                "r",
                encoding="utf-8"
            ) as f:
                return json.load(f)
        except Exception:
            pass

    return download_json(
        TOC_URL,
        TOC_FILE
    )


# ============================================================
# 生成页面清单
# ============================================================

def build_page_manifest(conn):
    """
    根据已经确认的卷 PID / 页数生成245页清单。

    NDL IIIF 官方规则：
        R0000001 = 第1コマ
        R0000002 = 第2コマ
        ...

    你已经实际验证：
        2555513/R0000002
        = 第一册完整第2页
    """

    total = 0

    for volume in VOLUMES:

        volume_no = volume["volume_no"]
        pid = volume["pid"]
        pages = volume["pages"]

        volume_dir = (
            IMAGE_DIR
            / f"{volume_no:02d}_{volume['title']}_{pid}"
        )

        volume_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        for page_no in range(1, pages + 1):

            resource_id = (
                f"R{page_no:07d}"
            )

            filename = (
                f"{page_no:04d}.jpg"
            )

            local_path = (
                volume_dir
                / filename
            )

            url = (
                f"{IIIF_BASE}/"
                f"{pid}/"
                f"{resource_id}/"
                f"full/full/0/default.jpg"
            )

            upsert_page(
                conn=conn,
                volume_no=volume_no,
                volume_pid=pid,
                page_no=page_no,
                resource_id=resource_id,
                filename=filename,
                url=url,
                local_path=str(
                    local_path.relative_to(
                        ARCHIVE_DIR
                    )
                )
            )

            total += 1

    logger.info(
        "页面清单建立完成：%d 页",
        total
    )


# ============================================================
# 查询页面
# ============================================================

def get_pages(
    conn,
    only_failed=False,
    only_pending=False,
    limit=None
):
    """
    获取需要处理的页面。

    正常模式：
        获取所有 status != verified 的页面

    --retry-failed：
        只获取 failed / http_error / missing / corrupt

    --test：
        在正常待下载集合中取前 N 页
    """

    sql = """
    SELECT
        id,
        volume_no,
        volume_pid,
        page_no,
        ndl_resource_id,
        download_url,
        local_path,
        status,
        retry_count
    FROM pages
    """

    if only_failed:

        sql += """
        WHERE status IN (
            'failed',
            'http_error',
            'missing',
            'corrupt'
        )
        """

    else:

        # 正常模式：
        # 所有没有成功验证的页面都应该继续处理
        sql += """
        WHERE status != 'verified'
        """

    sql += """
    ORDER BY volume_no, page_no
    """

    if limit:
        sql += f" LIMIT {int(limit)}"

    return conn.execute(sql).fetchall()

# ============================================================
# 页面下载
# ============================================================

def download_page(row):
    (
        db_id,
        volume_no,
        volume_pid,
        page_no,
        resource_id,
        download_url,
        local_path,
        old_status,
        old_retry_count
    ) = row

    final_path = (
        ARCHIVE_DIR / local_path
    )

    part_path = Path(
        str(final_path) + ".part"
    )

    final_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    # --------------------------------------------------------
    # 已经存在的文件
    # --------------------------------------------------------

    if final_path.exists():

        try:
            size = final_path.stat().st_size

            if size > 10000:

                sha = sha256_file(
                    final_path
                )

                return {
                    "id": db_id,
                    "status": "verified",
                    "http_status": 200,
                    "size": size,
                    "sha256": sha,
                    "retry_count": old_retry_count,
                    "message": "already_exists",
                }

        except Exception:
            pass

    session = get_session()

    last_error = None
    retry_count = old_retry_count or 0

    for attempt in range(
        1,
        DEFAULT_RETRIES + 1
    ):

        retry_count += 1

        try:

            logger.info(
                "[V%02d P%04d] 下载 %s",
                volume_no,
                page_no,
                resource_id
            )

            with session.get(
                download_url,
                stream=True,
                timeout=DEFAULT_TIMEOUT
            ) as r:

                http_status = r.status_code

                if r.status_code != 200:

                    last_error = (
                        f"HTTP {r.status_code}"
                    )

                    logger.warning(
                        "[V%02d P%04d] %s",
                        volume_no,
                        page_no,
                        last_error
                    )

                    if r.status_code in (
                        401,
                        403,
                        404
                    ):
                        break

                    time.sleep(
                        min(
                            2 ** attempt,
                            20
                        )
                    )

                    continue

                # ------------------------------------------------
                # 写入 .part
                # ------------------------------------------------

                with open(
                    part_path,
                    "wb"
                ) as f:

                    for chunk in r.iter_content(
                        chunk_size=CHUNK_SIZE
                    ):

                        if chunk:
                            f.write(chunk)

                size = part_path.stat().st_size

                if size < 10000:

                    last_error = (
                        f"文件异常小：{size} bytes"
                    )

                    logger.warning(
                        "[V%02d P%04d] %s",
                        volume_no,
                        page_no,
                        last_error
                    )

                    try:
                        part_path.unlink()
                    except Exception:
                        pass

                    time.sleep(2)

                    continue

                # ------------------------------------------------
                # SHA256
                # ------------------------------------------------

                sha = sha256_file(
                    part_path
                )

                # ------------------------------------------------
                # 原子替换
                # ------------------------------------------------

                part_path.replace(
                    final_path
                )

                logger.info(
                    "[V%02d P%04d] OK %.2f MB SHA=%s",
                    volume_no,
                    page_no,
                    size / 1024 / 1024,
                    sha[:16]
                )

                return {
                    "id": db_id,
                    "status": "verified",
                    "http_status": http_status,
                    "size": size,
                    "sha256": sha,
                    "retry_count": retry_count,
                    "message": "downloaded",
                }

        except Exception as e:

            last_error = str(e)

            logger.warning(
                "[V%02d P%04d] attempt=%d error=%s",
                volume_no,
                page_no,
                attempt,
                e
            )

            time.sleep(
                min(
                    2 ** attempt,
                    20
                )
            )

    return {
        "id": db_id,
        "status": "failed",
        "http_status": None,
        "size": 0,
        "sha256": None,
        "retry_count": retry_count,
        "message": last_error or "unknown",
    }


# ============================================================
# 数据库更新
# ============================================================

def update_download_result(
    conn,
    result
):

    conn.execute("""
    UPDATE pages
    SET
        status=?,
        http_status=?,
        file_size=?,
        sha256=?,
        retry_count=?,
        error_message=?,
        download_time=?
    WHERE id=?
    """, (
        result["status"],
        result["http_status"],
        result["size"],
        result["sha256"],
        result["retry_count"],
        result["message"],
        now(),
        result["id"]
    ))

    conn.commit()


# ============================================================
# 批量下载
# ============================================================

def download_all(
    conn,
    workers=DEFAULT_WORKERS,
    limit=None,
    only_failed=False
):

    rows = get_pages(
        conn,
        only_failed=only_failed,
        only_pending=not only_failed,
        limit=limit
    )

    total = len(rows)

    if total == 0:

        logger.info(
            "没有需要下载的页面。"
        )

        return

    logger.info(
        "准备处理 %d 页，workers=%d",
        total,
        workers
    )

    completed = 0
    failed = 0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers
    ) as executor:

        futures = {
            executor.submit(
                download_page,
                row
            ): row
            for row in rows
        }

        for future in concurrent.futures.as_completed(
            futures
        ):

            row = futures[future]

            try:
                result = future.result()

                update_download_result(
                    conn,
                    result
                )

                completed += 1

                if result["status"] != "verified":
                    failed += 1

                logger.info(
                    "进度 %d/%d | failed=%d",
                    completed,
                    total,
                    failed
                )

            except Exception as e:

                logger.exception(
                    "线程任务异常：%s",
                    e
                )

                failed += 1

    logger.info(
        "下载任务结束：total=%d failed=%d",
        total,
        failed
    )


# ============================================================
# 完整性检查
# ============================================================

def verify_all(conn):

    logger.info(
        "开始完整性检查..."
    )

    rows = conn.execute("""
    SELECT
        id,
        volume_no,
        volume_pid,
        page_no,
        local_path,
        sha256
    FROM pages
    ORDER BY volume_no, page_no
    """).fetchall()

    ok = 0
    missing = 0
    bad = 0

    for row in rows:

        (
            db_id,
            volume_no,
            volume_pid,
            page_no,
            local_path,
            old_sha
        ) = row

        path = ARCHIVE_DIR / local_path

        if not path.exists():

            conn.execute("""
            UPDATE pages
            SET
                status='missing',
                verify_time=?
            WHERE id=?
            """, (
                now(),
                db_id
            ))

            missing += 1
            continue

        try:

            size = path.stat().st_size

            if size < 10000:
                raise ValueError(
                    "文件过小"
                )

            sha = sha256_file(path)

            if old_sha and sha != old_sha:

                conn.execute("""
                UPDATE pages
                SET
                    status='corrupt',
                    file_size=?,
                    sha256=?,
                    verify_time=?,
                    error_message=?
                WHERE id=?
                """, (
                    size,
                    sha,
                    now(),
                    "SHA256 mismatch",
                    db_id
                ))

                bad += 1

                continue

            conn.execute("""
            UPDATE pages
            SET
                status='verified',
                file_size=?,
                sha256=?,
                verify_time=?
            WHERE id=?
            """, (
                size,
                sha,
                now(),
                db_id
            ))

            ok += 1

        except Exception as e:

            conn.execute("""
            UPDATE pages
            SET
                status='corrupt',
                verify_time=?,
                error_message=?
            WHERE id=?
            """, (
                now(),
                str(e),
                db_id
            ))

            bad += 1

    conn.commit()

    logger.info(
        "完整性检查完成：OK=%d missing=%d corrupt=%d",
        ok,
        missing,
        bad
    )

    return ok, missing, bad


# ============================================================
# SHA256SUMS
# ============================================================

def generate_sha256(conn):

    rows = conn.execute("""
    SELECT
        volume_no,
        page_no,
        local_path,
        sha256
    FROM pages
    WHERE status='verified'
    ORDER BY volume_no, page_no
    """).fetchall()

    with open(
        SHA_FILE,
        "w",
        encoding="utf-8"
    ) as f:

        for (
            volume_no,
            page_no,
            local_path,
            sha
        ) in rows:

            f.write(
                f"{sha}  {local_path}\n"
            )

    logger.info(
        "SHA256SUMS 已生成：%s",
        SHA_FILE
    )


# ============================================================
# HTML报告
# ============================================================

def generate_report(conn):

    total = conn.execute(
        "SELECT COUNT(*) FROM pages"
    ).fetchone()[0]

    verified = conn.execute(
        "SELECT COUNT(*) FROM pages WHERE status='verified'"
    ).fetchone()[0]

    failed = conn.execute(
        "SELECT COUNT(*) FROM pages WHERE status='failed'"
    ).fetchone()[0]

    missing = conn.execute(
        "SELECT COUNT(*) FROM pages WHERE status='missing'"
    ).fetchone()[0]

    corrupt = conn.execute(
        "SELECT COUNT(*) FROM pages WHERE status='corrupt'"
    ).fetchone()[0]

    rows = conn.execute("""
    SELECT
        volume_no,
        volume_pid,
        page_no,
        status,
        file_size,
        sha256,
        download_time,
        verify_time,
        error_message
    FROM pages
    ORDER BY volume_no, page_no
    """).fetchall()

    html = []

    html.append("""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>山海經 NDL 数字归档报告</title>

<style>
body {
    font-family:
        "Microsoft YaHei",
        "Noto Sans CJK SC",
        sans-serif;
    margin: 30px;
}

table {
    border-collapse: collapse;
    width: 100%;
}

th, td {
    border: 1px solid #ccc;
    padding: 6px;
}

th {
    background: #eee;
}

.ok {
    color: green;
}

.bad {
    color: red;
}

.small {
    font-size: 12px;
}
</style>

</head>
<body>
""")

    html.append(
        "<h1>《山海經18卷》数字归档报告</h1>"
    )

    html.append(
        f"<p>生成时间：{now()}</p>"
    )

    html.append(
        f"<p>Root PID：{ROOT_PID}</p>"
    )

    html.append(
        f"<p>总页数：{total}</p>"
    )

    html.append(
        f"<p class='ok'>完整：{verified}</p>"
    )

    html.append(
        f"<p class='bad'>失败：{failed}</p>"
    )

    html.append(
        f"<p class='bad'>缺失：{missing}</p>"
    )

    html.append(
        f"<p class='bad'>损坏：{corrupt}</p>"
    )

    html.append("""
<table>
<tr>
<th>册</th>
<th>PID</th>
<th>页码</th>
<th>状态</th>
<th>大小</th>
<th>SHA256</th>
<th>下载时间</th>
<th>验证时间</th>
<th>错误</th>
</tr>
""")

    for row in rows:

        (
            volume_no,
            volume_pid,
            page_no,
            status,
            file_size,
            sha,
            download_time,
            verify_time,
            error_message
        ) = row

        css = (
            "ok"
            if status == "verified"
            else "bad"
        )

        html.append(
            f"""
<tr>
<td>{volume_no}</td>
<td>{volume_pid}</td>
<td>{page_no}</td>
<td class="{css}">{status}</td>
<td>{file_size:,}</td>
<td class="small">{sha or ''}</td>
<td>{download_time or ''}</td>
<td>{verify_time or ''}</td>
<td>{error_message or ''}</td>
</tr>
"""
        )

    html.append("""
</table>
</body>
</html>
""")

    REPORT_FILE.write_text(
        "".join(html),
        encoding="utf-8"
    )

    logger.info(
        "HTML报告已生成：%s",
        REPORT_FILE
    )


# ============================================================
# PDF
# ============================================================

def generate_pdf(conn):

    try:
        from PIL import Image
    except ImportError:

        logger.warning(
            "未安装 Pillow，跳过 PDF。"
            "安装：pip install pillow"
        )

        return

    output_pdf = (
        PDF_DIR
        / "山海經18卷_明刊_NDL归档版.pdf"
    )

    rows = conn.execute("""
    SELECT
        volume_no,
        page_no,
        local_path
    FROM pages
    WHERE status='verified'
    ORDER BY volume_no, page_no
    """).fetchall()

    if len(rows) != TOTAL_PAGES:

        logger.warning(
            "当前只有 %d/%d 页完整，暂不生成最终 PDF。",
            len(rows),
            TOTAL_PAGES
        )

        return

    images = []

    logger.info(
        "开始准备 PDF，共 %d 页...",
        len(rows)
    )

    for (
        volume_no,
        page_no,
        local_path
    ) in rows:

        path = ARCHIVE_DIR / local_path

        try:
            img = Image.open(path)

            if img.mode != "RGB":
                img = img.convert("RGB")

            images.append(img)

        except Exception as e:

            logger.error(
                "PDF读取失败 V%d P%d: %s",
                volume_no,
                page_no,
                e
            )

            return

    first = images[0]
    rest = images[1:]

    first.save(
        output_pdf,
        "PDF",
        resolution=300.0,
        save_all=True,
        append_images=rest
    )

    for img in images:
        try:
            img.close()
        except Exception:
            pass

    logger.info(
        "PDF生成完成：%s",
        output_pdf
    )


# ============================================================
# 档案说明
# ============================================================

def generate_readme():

    readme = DOC_DIR / "README.txt"

    text = f"""
《山海經18卷》明刊古籍数字典藏
====================================

NDL Root PID:
{ROOT_PID}

题名:
山海經18卷

作者:
晉郭璞傳

绘图:
明蒋應鎬畫

出版:
明刊

馆藏:
国立国会図書館

权限:
Public Domain Mark / Internet公开

------------------------------------
卷册
------------------------------------

第一册:
PID 2555513
63页

第二册:
PID 2555514
85页

第三册:
PID 2555515
49页

第四册:
PID 2555516
48页

总计:
245页

------------------------------------
图像来源
------------------------------------

NDL IIIF Image API

格式：

https://dl.ndl.go.jp/api/iiif/{{PID}}/R{{PAGE:07d}}/full/full/0/default.jpg

例如：

https://dl.ndl.go.jp/api/iiif/2555513/R0000002/full/full/0/default.jpg

经验证：
2555513 / R0000002
对应第一册完整第2页。

------------------------------------
归档原则
------------------------------------

1. 原始影像不修改。
2. 原始文件使用 JPG 保存。
3. 每页计算 SHA256。
4. SQLite 保存页面级状态。
5. 下载失败可以重新执行。
6. 已经验证的页面不会重复下载。
7. SHA256SUMS.txt 用于长期完整性验证。
8. archive_report.html 用于查看归档状态。
9. PDF属于衍生文件，不能替代原始页面影像。
10. 00_原始资料目录保存来源信息。

生成时间:
{now()}
"""

    readme.write_text(
        text.strip() + "\n",
        encoding="utf-8"
    )


# ============================================================
# 状态
# ============================================================

def show_status(conn):

    rows = conn.execute("""
    SELECT status, COUNT(*)
    FROM pages
    GROUP BY status
    ORDER BY status
    """).fetchall()

    print()
    print("=" * 60)
    print("NDL 山海經归档状态")
    print("=" * 60)

    total = 0

    for status, count in rows:

        print(
            f"{status:<15} {count:>5}"
        )

        total += count

    print("-" * 60)
    print(
        f"{'TOTAL':<15} {total:>5}"
    )

    print("=" * 60)


# ============================================================
# 主程序
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="NDL 山海經18卷数字归档器 V5"
    )

    parser.add_argument(
        "--test",
        type=int,
        default=0,
        help="只测试下载前N页"
    )

    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="只重新下载失败页面"
    )

    parser.add_argument(
        "--verify",
        action="store_true",
        help="只执行完整性检查"
    )

    parser.add_argument(
        "--status",
        action="store_true",
        help="显示当前归档状态"
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="并发下载线程数，默认4"
    )

    parser.add_argument(
        "--no-pdf",
        action="store_true",
        help="不生成PDF"
    )

    args = parser.parse_args()

    setup_logging()

    ensure_dirs()

    logger.info(
        "=" * 70
    )

    logger.info(
        "NDL《山海經18卷》数字归档器 V5"
    )

    logger.info(
        "Root PID = %s",
        ROOT_PID
    )

    logger.info(
        "Total pages = %d",
        TOTAL_PAGES
    )

    logger.info(
        "=" * 70
    )

    conn = init_db()

    # --------------------------------------------------------
    # 保存基础说明
    # --------------------------------------------------------

    generate_readme()

    # --------------------------------------------------------
    # TOC
    # --------------------------------------------------------

    toc = download_toc()

    if toc is not None:

        logger.info(
            "TOC 已保存：%s",
            TOC_FILE
        )

    # --------------------------------------------------------
    # 建立245页清单
    # --------------------------------------------------------

    build_page_manifest(conn)

    # --------------------------------------------------------
    # status
    # --------------------------------------------------------

    if args.status:

        show_status(conn)

        conn.close()

        return

    # --------------------------------------------------------
    # verify
    # --------------------------------------------------------

    if args.verify:

        verify_all(conn)

        generate_sha256(conn)

        generate_report(conn)

        show_status(conn)

        conn.close()

        return

    # --------------------------------------------------------
    # 下载
    # --------------------------------------------------------

    limit = (
        args.test
        if args.test > 0
        else None
    )

    download_all(
        conn,
        workers=max(1, args.workers),
        limit=limit,
        only_failed=args.retry_failed
    )

    # --------------------------------------------------------
    # 完整性检查
    # --------------------------------------------------------

    verify_all(conn)

    # --------------------------------------------------------
    # SHA256
    # --------------------------------------------------------

    generate_sha256(conn)

    # --------------------------------------------------------
    # HTML
    # --------------------------------------------------------

    generate_report(conn)

    # --------------------------------------------------------
    # PDF
    # --------------------------------------------------------

    if not args.no_pdf:
        generate_pdf(conn)

    # --------------------------------------------------------
    # 状态
    # --------------------------------------------------------

    show_status(conn)

    conn.close()

    logger.info(
        "全部任务完成。"
    )


if __name__ == "__main__":
    main()