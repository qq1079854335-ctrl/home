#!/usr/bin/env python3
"""29_api_pluggable_google.py
Google Custom Search + DeepSeekAPI
- 从CSV或XLSX文件读取CEO信息
- 固定搜索时间范围：1995-2025
- 自动在 query 加年份
- 按发布时间排序
- 输出所有找到的项目，不过滤
- CSV列顺序：date, ceo, company, office_level, candidate_guess, headline, outlet, url
- 每100个CEO保存一个文件
"""

from __future__ import annotations
import argparse
import csv
import json
import time
import requests
import re
import sys
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

# 支持术语列表
ENDORSEMENT_TERMS = [
    "endorses", "endorsed",  "throws support", "throws his support",
    "throws her support", "urged voters to back", "urged voters to support", "endorsing"
]

OFFICE_HINTS = {
    "president": ["president", "white house", "presidential", "presidential race"],
    "senate": ["senate", "senator", "u.s. senate", "senatorship"],
    "house": ["house", "representative", "congressman", "congresswoman"],
}

def read_ceos_from_file(file_path: str) -> List[str]:
    """从CSV或XLSX文件读取CEO信息，返回CEO姓名列表"""
    ceos = []
    try:
        # 根据文件扩展名选择读取方式
        if file_path.lower().endswith('.csv'):
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ceos.append(row['exec_fullname'])
        elif file_path.lower().endswith(('.xlsx', '.xls')):
            # 使用pandas读取Excel文件
            df = pd.read_excel(file_path)
            if 'exec_fullname' in df.columns:
                ceos = df['exec_fullname'].dropna().tolist()
            else:
                print("Error: 'exec_fullname' column not found in the Excel file")
                sys.exit(1)
        else:
            print(f"Error: Unsupported file format: {file_path}")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        sys.exit(1)
    return ceos

def build_query(ceo_name: str) -> str:
    terms = " OR ".join(f'"{t}"' for t in ENDORSEMENT_TERMS)
    # 固定年份范围：1995-2025
    years = "1995..2025"
    return f'"{ceo_name}" AND ({terms}) {years}'

def normalize_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        return dt.date().isoformat()
    except Exception:
        return None

def extract_candidate(text: str) -> Optional[str]:
    if not text:
        return None
    patterns = [
        r"(?:endorses|endorsed|backs|backed|endorsing)\s+([A-Z][a-zA-Z.\-']+(?:\s+[A-Z][a-zA-Z.\-']+)*)",
        r"(?:throws support (?:behind|for))\s+([A-Z][a-zA-Z.\-']+(?:\s+[A-Z][a-zA-Z.\-']+)*)",
        r"(?:urged voters to (?:back|support))\s+([A-Z][a-zA-Z.\-']+(?:\s+[A-Z][a-zA-Z.\-']+)*)",
    ]
    for p in patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            return re.sub(r"'s$", "", name)
    return None

def classify_office(text: str) -> Optional[str]:
    if not text:
        return None
    txt = text.lower()
    for office, hints in OFFICE_HINTS.items():
        for h in hints:
            if h in txt:
                return office
    return None

# ------------------------------
# Backend
# ------------------------------
class BaseBackend:
    def search(self, ceo: str, query: str, since: Optional[str], until: Optional[str], maxrecords: int) -> List[Dict[str, Any]]:
        raise NotImplementedError

class HTTPJSONBackend(BaseBackend):
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def _fill(self, template):
        if template is None:
            return None
        if isinstance(template, dict):
            return {k: self._fill(v) for k, v in template.items()}
        if isinstance(template, list):
            return [self._fill(x) for x in template]
        if isinstance(template, str):
            return template.format(**self._fmt_context)
        return template

    def _traverse(self, obj: Any, path: str):
        if not path:
            return obj
        parts = path.split(".")
        cur = obj
        try:
            for p in parts:
                if isinstance(cur, list):
                    if p.isdigit():
                        cur = cur[int(p)]
                    else:
                        cur = [c.get(p) if isinstance(c, dict) else None for c in cur]
                else:
                    cur = cur.get(p)
            return cur
        except Exception:
            return None

    def search(self, ceo: str, query: str, since: Optional[str], until: Optional[str], maxrecords: int) -> List[Dict[str, Any]]:
        cfg = self.config
        self._fmt_context = {
            "query": query,
            "ceo": ceo,
            "since": since or "",
            "until": until or "",
            "maxrecords": maxrecords,
        }
        url = cfg.get("url")
        method = cfg.get("method", "GET").upper()
        headers = cfg.get("headers") or {}
        params = self._fill(cfg.get("params") or {})
        # 强制按日期排序
        params["sort"] = "date"
        json_body = self._fill(cfg.get("json"))
        timeout = cfg.get("timeout", 30)
        try:
            if method == "GET":
                r = requests.get(url, params=params, headers=headers, timeout=timeout)
            else:
                r = requests.request(method, url, params=params, json=json_body, headers=headers, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            items_path = cfg.get("items_path") or ""
            items = self._traverse(data, items_path)
            if items is None:
                return []
            if isinstance(items, dict):
                items = [items]
            mappings: Dict[str, str] = cfg.get("mappings") or {}
            out = []
            for it in items[:maxrecords]:
                if not isinstance(it, dict):
                    continue
                headline = it.get(mappings.get("headline", "title")) or it.get("title") or it.get("headline")
                urlv = it.get(mappings.get("url", "link")) or it.get("url") or it.get("link")
                datev = it.get(mappings.get("date", "publishedAt")) or it.get("date")
                outlet = it.get(mappings.get("outlet", "source")) or it.get("source")
                content = it.get("content") or it.get("description") or headline
                out.append({"headline": headline, "url": urlv, "date": normalize_date(datev),
                            "outlet": outlet, "content": content})
            return out
        except Exception as e:
            print(f"HTTP backend error for {ceo}: {e}", file=sys.stderr)
            return []

# ------------------------------
# DeepSeekAPI
# ------------------------------
def extract_with_llm(text: str) -> Dict[str, Optional[str]]:
    return {
        "company": None,
        "candidate_guess": extract_candidate(text),
        "office_level": classify_office(text)
    }

# ------------------------------
# Main pipeline
# ------------------------------
def process_ceos(ceos: List[str], backend: BaseBackend, maxrecords: int, output_base: str, chunk_size: int = 100):
    """处理CEO列表，每chunk_size个CEO保存一个文件"""
    all_rows = []
    
    # 创建输出目录（如果不存在）
    output_dir = os.path.dirname(output_base)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 获取基础文件名和扩展名
    base_name, ext = os.path.splitext(output_base)
    if not ext:
        ext = ".csv"
    
    # 分批处理CEO
    for i in range(0, len(ceos), chunk_size):
        chunk = ceos[i:i+chunk_size]
        chunk_number = i // chunk_size + 1
        output_file = f"{base_name}_{chunk_number}{ext}"
        
        print(f"Processing chunk {chunk_number}: CEOs {i+1} to {min(i+chunk_size, len(ceos))}")
        
        rows = []
        for ceo_name in chunk:
            # 固定时间范围：1995-2025
            since = "1995-01-01"
            until = "2025-12-31"
            
            q = build_query(ceo_name)
            print(f"  Searching for CEO: {ceo_name}")
            
            items = backend.search(ceo=ceo_name, query=q, since=since, until=until, maxrecords=maxrecords)
            print(f"  Found {len(items)} raw items for {ceo_name}")
            
            # 处理所有找到的项目，不过滤
            for it in items:
                content = it.get("content") or it.get("headline") or ""
                llm_res = extract_with_llm(content)
                rows.append({
                    "date": it.get("date"),
                    "ceo": ceo_name,
                    "company": llm_res.get("company"),
                    "office_level": llm_res.get("office_level"),
                    "candidate_guess": llm_res.get("candidate_guess"),
                    "headline": it.get("headline"),
                    "outlet": it.get("outlet"),
                    "url": it.get("url"),
                })
            time.sleep(3.0)  # 避免请求过于频繁
        
        if not rows:
            print(f"No rows collected for chunk {chunk_number}, skipping.")
            continue
        
        # 保存当前块的结果
        df = pd.DataFrame(rows)
        df = df.drop_duplicates(subset=["headline", "url", "date", "ceo"])
        df = df.sort_values(["date", "ceo"], ascending=[False, True])
        
        # 按照指定顺序排列列
        columns_order = ["date", "ceo", "company", "office_level", "candidate_guess", "headline", "outlet", "url"]
        df = df[columns_order]
        
        df.to_csv(output_file, index=False, quoting=csv.QUOTE_MINIMAL)
        print(f"Wrote {len(df)} rows to {output_file}")
        
        # 将当前块的结果添加到总结果中
        all_rows.extend(rows)
    
    # 如果需要，也可以保存一个包含所有结果的合并文件
    if all_rows:
        all_output_file = f"{base_name}_all{ext}"
        all_df = pd.DataFrame(all_rows)
        all_df = all_df.drop_duplicates(subset=["headline", "url", "date", "ceo"])
        all_df = all_df.sort_values(["date", "ceo"], ascending=[False, True])
        all_df = all_df[columns_order]
        all_df.to_csv(all_output_file, index=False, quoting=csv.QUOTE_MINIMAL)
        print(f"Wrote {len(all_df)} rows to combined file {all_output_file}")
    
    return all_rows

# ------------------------------
# CLI
# ------------------------------
def load_backend(backend_name: str, backend_config_path: Optional[str]) -> BaseBackend:
    if backend_name == "http":
        if not backend_config_path:
            raise ValueError("HTTP backend requires --backend-config pointing to a JSON file")
        with open(backend_config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        return HTTPJSONBackend(cfg)
    raise ValueError(f"Unknown backend: {backend_name}")

def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Search endorsements for CEOS using Google API + DeepSeekAPI")
    p.add_argument("--ceo-file", help="Path to CSV or XLSX file containing CEO information")
    p.add_argument("--test-musk", action="store_true", help="Test with Elon Musk")
    p.add_argument("--maxrecords", type=int, default=10, help="Maximum records per CEO")
    p.add_argument("--chunk-size", type=int, default=100, help="Number of CEOs per output file")
    p.add_argument("--backend", choices=["http"], default="http")
    p.add_argument("--backend-config", required=True, help="Path to backend configuration JSON file")
    p.add_argument("--output", default="ceo_endorsements.csv", help="Base output CSV file path")
    return p.parse_args(argv)

def main(argv=None):
    args = parse_args(argv)
    
    # 加载后端配置
    backend = load_backend(args.backend, args.backend_config)
    
    # 确定要处理的CEO列表
    if args.test_musk:
        # 测试模式：只处理Elon Musk
        ceos = ["Elon Musk"]
        print("Testing with Elon Musk")
    elif args.ceo_file:
        # 从文件读取CEO信息
        ceos = read_ceos_from_file(args.ceo_file)
        print(f"Loaded {len(ceos)} CEOs from {args.ceo_file}")
    else:
        print("Error: Either --ceo-file or --test-musk must be specified")
        sys.exit(1)
    
    # 处理每个CEO
    process_ceos(ceos, backend, args.maxrecords, args.output, args.chunk_size)

if __name__ == '__main__':
    main()