import os
import sys
import time
import urllib.request

DOWNLOADS_DIR = os.path.join(os.path.dirname(__file__), "..", "docker", "base", "downloads")
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

PACKAGES = [
    {
        "name": "hadoop-3.4.3.tar.gz",
        "urls": [
            "https://mirrors.huaweicloud.com/apache/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz",
            "https://dlcdn.apache.org/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz",
            "https://archive.apache.org/dist/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz"
        ]
    },
    {
        "name": "spark-3.5.4-bin-hadoop3.tgz",
        "urls": [
            "https://mirrors.huaweicloud.com/apache/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz",
            "https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz"
        ]
    },
    {
        "name": "apache-hive-3.1.3-bin.tar.gz",
        "urls": [
            "https://mirrors.huaweicloud.com/apache/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz",
            "https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz"
        ]
    },
    {
        "name": "postgresql-42.7.3.jar",
        "urls": [
            "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
        ]
    },
    {
        "name": "clickhouse-jdbc-0.6.3-all.jar",
        "urls": [
            "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3-all.jar"
        ]
    }
]

def download_file(target_path, urls):
    if os.path.exists(target_path) and os.path.getsize(target_path) > 1000:
        print(f"[OK] Already exists: {os.path.basename(target_path)} ({os.path.getsize(target_path) / 1024 / 1024:.2f} MB)")
        return True

    for url in urls:
        print(f"Downloading {os.path.basename(target_path)} from: {url}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp, open(target_path + ".tmp", "wb") as out_file:
                total_size = int(resp.headers.get("Content-Length", 0))
                downloaded = 0
                chunk_size = 1024 * 1024  # 1MB
                t0 = time.time()

                while True:
                    chunk = resp.read(chunk_size)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    downloaded += len(chunk)
                    elapsed = time.time() - t0
                    speed = (downloaded / 1024 / 1024) / (elapsed + 0.001)
                    if total_size > 0:
                        pct = (downloaded / total_size) * 100
                        print(f"\r  Progress: {pct:.1f}% ({downloaded / 1024 / 1024:.1f}/{total_size / 1024 / 1024:.1f} MB) - {speed:.2f} MB/s", end="", flush=True)
                    else:
                        print(f"\r  Downloaded: {downloaded / 1024 / 1024:.1f} MB - {speed:.2f} MB/s", end="", flush=True)

                print()

            os.rename(target_path + ".tmp", target_path)
            print(f"[DONE] {os.path.basename(target_path)} saved successfully!")
            return True
        except Exception as e:
            print(f"\n[FAIL] Failed downloading from {url}: {e}")
            if os.path.exists(target_path + ".tmp"):
                os.remove(target_path + ".tmp")

    return False

def main():
    print("Starting package pre-download...")
    for pkg in PACKAGES:
        target = os.path.join(DOWNLOADS_DIR, pkg["name"])
        success = download_file(target, pkg["urls"])
        if not success:
            print(f"CRITICAL: Failed to download {pkg['name']}")
            sys.exit(1)
    print("\nAll packages downloaded successfully into docker/base/downloads/!")

if __name__ == "__main__":
    main()
