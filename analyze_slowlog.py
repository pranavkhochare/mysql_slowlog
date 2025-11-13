#!/usr/bin/env python3

import os
import glob
from datetime import datetime, timedelta
import shutil
import gzip
import subprocess
from dotenv import load_dotenv
import mysql.connector
import json
import requests
import sys
import re
import time
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP
import markdown
from langchain_ollama import ChatOllama
# from langchain_core.prompts import PromptTemplate
import tempfile
# from collections import defaultdict
import socket


load_dotenv()

# env variables for script
username = os.getenv("db_username")
password = os.getenv("db_password")
host     = os.getenv("db_hostname")
URL      = os.getenv("LLM_API_URL")
MODEL    = os.getenv("LLM_MODEL")
# mail env variables
HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT"))
SENDER = os.getenv("SENDER")
USER = os.getenv("MAIL_USER")
PASSWORD = os.getenv("PASSWORD")
RECIPIENT = ["yyyy@gmail.com"]
CC = ["abc@gmail.com"]
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

logger = logging.getLogger(__name__)

def set_logger():
    if logger.handlers:  # avoid duplicate handlers
        return
    logger.setLevel(logging.INFO)
    log_dir = './logs'
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, 'analyze-mysql-slow.log')
    handler = logging.FileHandler(path, encoding='utf-8')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(threadName)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def post_teams_message(text):
    json_data = {
      "text": text
    }
    try:
        response = requests.post(WEBHOOK_URL, json=json_data, timeout=20)
        if response.status_code in [200, 201, 202]:
            logger.info("Teams notification sent successfully")
        else:
            logger.warning(f"Teams notification failed with status code {response.status_code}, please check Teams workflow logs or webhook URL")
    except requests.exceptions.RequestException as e:
        logger.error("Teams notification exception: %s", e)


def failed_script(message):
    logger.error(message)
    logger.error(f"Query analysis aborted.")
    post_teams_message(message)
    sys.exit(1)

def get_host_id():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = socket.gethostname()
    finally:
        try: s.close()
        except: pass
    return ip

ipaddr = get_host_id()

def copy_log_files(conn, combined_logs="combined_slow_logs.log"):
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')
    cursor = conn.cursor()
    cursor.execute("SHOW GLOBAL VARIABLES LIKE 'slow_query_log_file';")
    result = cursor.fetchone()

    if result and result[1]:
        slow_log_path = result[1]
        slow_log_dir = os.path.dirname(slow_log_path)
    else:
        failed_script(f"Not able to get slow log path on host:{ipaddr}")

    current_dir = os.getcwd()
    dest_dir = os.path.join(current_dir, "slow-log")

    if os.path.isdir(dest_dir):
        shutil.rmtree(dest_dir)
    os.makedirs(dest_dir)

    pattern = os.path.join(f"{slow_log_dir}/*{yesterday}*")

    matching_files = glob.glob(pattern)

    if matching_files:
        for file_path in matching_files:
            file_name = os.path.basename(file_path)
            dest_path = os.path.join(dest_dir, file_name)
            shutil.copy2(file_path, dest_path)
    else:
        failed_script(f"No slow logs files found for yesterday {yesterday} aborting script on host {ipaddr}")

    with open(combined_logs, "w", encoding="utf-8") as outputfile:
        for root, _, files in os.walk(dest_dir):
            for filename in sorted(files):
                if filename.endswith(".gz"):
                    file_path = os.path.join(root, filename)
                    try:
                        with gzip.open(file_path, 'rt', encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                outputfile.write(line)
                    except Exception as e:
                        logger.error("Failed to read {file_path}: {e}")
    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)
    
    cursor.execute("SHOW GLOBAL VARIABLES LIKE 'version';")
    version = cursor.fetchone()[1]
    
    return combined_logs,version

def normalize_query(query: str) -> str:
    """Collapse whitespace in query."""
    return ' '.join(query.strip().split())

def extract_and_sort_slow_queries(
    input_file: str,
    output_file="filtered_slow.json",
    top_n: int = 10
) -> str:
    
    db_pattern = re.compile(r"Databases\s+(\S+)")
    current_db = None
    current_query = []
    db_queries = {}
    
    pt_cmd = [
        "pt-query-digest",                
        "--group-by", "fingerprint",
        "--order-by", "Query_time:sum",
        f"--limit={top_n}",
        "--filter", '($event->{arg} =~ m/^SELECT/i)',
        input_file
    ]

    with tempfile.TemporaryDirectory() as tmpd:
        report_path = os.path.join(tmpd, "pt_report.log")
        try:
            with open(report_path, "w", encoding="utf-8") as rpt:
                subprocess.run(pt_cmd, check=True, stdout=rpt, stderr=subprocess.PIPE, text=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"pt-query-digest error:\n{e.stderr}") from e
        
        with open(report_path, "r") as f:
        
            for line in f:
                stripped = line.strip()
        
                db_match = db_pattern.search(stripped)
                if db_match:
                    current_db = db_match.group(1)
                    if current_db not in db_queries:
                        db_queries[current_db] = []
                    continue
        
                # Skip comments and blanks
                if not stripped or stripped.startswith('#'):
                    continue
        
                # Collect query text
                current_query.append(stripped)
        
                # Query termination (ends with ";" or "\G")
                if stripped.endswith(";") or stripped.endswith("\\G"):
                    full_query = ' '.join(current_query)
                    normalized = normalize_query(full_query)
                    if current_db:
                        if normalized not in db_queries[current_db]:
                            db_queries[current_db].append(normalized.replace("\\G",""))
                    current_query = []
            
        for db in db_queries:
            db_queries[db] = db_queries[db][:10]

        with open(output_file, "w", encoding='utf-8') as out:
            json.dump(db_queries, out, indent=2)
    
    return output_file

def strip_think(text: str) -> str:
    # Removes <think> ... </think> (case-insensitive, multi-line)
    return re.sub(r"\s*<think>.*?</think>\s*", "", text, flags=re.DOTALL | re.IGNORECASE)

def get_llm_output(query ,explain_output,ver) -> str:

    llm = ChatOllama(
    model = MODEL,
    validate_model_on_init = True,
    base_url=URL
    )

    template = (
        "You are a MySQL query optimization expert.\n"
        f"MySQL version: {ver}\n"
        "EXPLAIN FORMAT=JSON output:\n"
        f"{explain_output}\n\n"
        "Query:\n"
        f"{query}\n\n"
        "Goal:\n"
        "Rules"
        "Be plain and concrete; no DBA jargon in main text."
        "Do not speculate. Only reference columns visible in the query/EXPLAIN."
        "Prefer low-risk changes first (indexes, small rewrites). Avoid schema changes unless clearly necessary."
        "Always say to test in a non-prod environment."
        "Output format (Bold sub-points):"
        "Bottlenecks in query"
        "impact on server resources and database"
        "Fixes (max 3 to 4 bullets)"
        "suggested optimized query if query is large write only parts which can be optimized"
        "Only produce the sections above. No preamble or extra commentary."
    )
    
    try:
        message = llm.invoke(template)
        suggestion = strip_think(getattr(message, "content", "") or str(message))
        return suggestion or ""
    except Exception as e:
        logger.error(f"LLM exception: {e}")
        return ""

def fetch_explain_output(cursor, query, database):
    try:
        if query.lower().startswith("select"):
            cursor.execute(f"use `{database}`")
            cursor.execute(f"EXPLAIN FORMAT=JSON {query};")
            row = cursor.fetchone()
            return row[0] if row else ""
            # return cursor.fetchall()
        return ""
    except mysql.connector.Error as e:
        logger.error(f"Error executing EXPLAIN for {query}: {e}")
        return ""

def analyze_query_with_llm(query, explain_output,ver):
    try:
        return get_llm_output(query, explain_output,ver)
    except Exception as e:
        logger.error(f"Error getting LLM output for {query}: {e}")
        return None

def process_query(cursor, query, database,ver):
    explain_output = fetch_explain_output(cursor, query, database)
    
    if explain_output:
        return analyze_query_with_llm(query, explain_output,ver)
    return None

def get_query_optimization_output(slowlog_file, ver, conn):
    cursor = conn.cursor()
    ai_output = [f"### Slow query suggestion for server {ipaddr}\n\n"
                 f"### Note: The below AI-generated query optimization suggestions are for guidance only. Please validate in a test environment before applying to production systems.\n{'*'*60}\n\n"]

    with open(slowlog_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    for db, queries in data.items():
        for q in queries:
            normalized_query = re.sub(r"\s+", " ", q).strip()
            ai_output_for_query = process_query(cursor, normalized_query, db, ver)
            time.sleep(5)
            if ai_output_for_query:
                ai_output.append(
                    f"**DB:{db} Query:**\n {normalized_query}\n"
                    f"\n{ai_output_for_query.strip()}\n{'*'*60}\n"
                )

    cursor.close()
    return ai_output

def write_to_file(suggestions: list, output_file: str = "ai_query_suggestion.md") -> str:
    with open("temp_output.md", "w",encoding="utf-8") as f:
        f.writelines(suggestions)
    output = []
    with open("temp_output.md","r",encoding='utf-8') as f:
        for line in f:
            output.append(line.strip() + "\n")
    
    with open(output_file,"w",encoding='utf-8') as f:
        f.writelines(output)
    os.remove("temp_output.md")

    return output_file


def sender_mail(ai_output_file: str):

    with open(ai_output_file) as file:
        message = file.read()

    server = SMTP(host=HOST, port=PORT)
    server.connect(host=HOST, port=PORT)
    server.ehlo()
    server.login(user=USER, password=PASSWORD)
    
    multipart_msg = MIMEMultipart("alternative")
    
    multipart_msg["Subject"] = f"suggested slow query optimzation on {ipaddr} for date {(datetime.now() - timedelta(1)).strftime('%d-%m-%Y')}"
    multipart_msg["From"] = SENDER
    multipart_msg["To"] =  ", ".join(RECIPIENT)
    if CC:
        multipart_msg["Cc"] = ", ".join(CC)
    
    all_recipients = RECIPIENT + CC
    
    text = message
    html = markdown.markdown(text)
    
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")
    multipart_msg.attach(part1)
    multipart_msg.attach(part2)
    
    try:
        server.sendmail(SENDER, all_recipients, multipart_msg.as_string())
        logger.info(f"sent mail successfully to users to {all_recipients}")
    except Exception as e :
        logger.error(f"Failed to send mail {e}") 
    return 'Sent email successfully!'

def main():
    try:
        conn = mysql.connector.connect(
                host=host,
                user=username,
                password=password
        )
        
        if shutil.which("pt-query-digest") is None:
            failed_script("pt-query-digest not found in PATH. Install Percona Toolkit.")

        if os.path.exists("./logs/analyze-mysql-slow.log"):
            os.remove("./logs/analyze-mysql-slow.log")
        set_logger()
        logger.info("Starting the slow query log optimization process")

        if os.path.exists(".env"):
            logger.info(".env file present")
        else:
            failed_script(f".env file not found please create env file with connection details on host {ipaddr}")

        combined_logs, mysql_version = copy_log_files(conn)

        logger.info("Slow logs copied and combined.")

        filtered_logs = extract_and_sort_slow_queries(combined_logs)

        os.remove(combined_logs)

        logger.info(f"Top slow queries extracted to {filtered_logs}.")

        ai_suggestions = get_query_optimization_output(filtered_logs,mysql_version, conn)

        os.remove(filtered_logs)
        if ai_suggestions:
            output_file = write_to_file(ai_suggestions)
            output_file_size = os.stat(output_file).st_size
            if output_file_size > 500:
                logger.info("Optimization suggestions written to file.")
                sender_mail(output_file)
            else:
                logger.info(f"Output file size {output_file_size} bytes is less than expected.")   
            os.remove(output_file)
        else:
            failed_script(f"No AI suggestions received on host: {ipaddr}")
    except Exception as e:
        failed_script(f"Exception occurred during execution: {e}, on host {ipaddr}")
    finally:
        conn.close()
        logger.info("Database connection closed.")

if __name__ == "__main__":
    main()