# -*- coding: utf-8 -*-
"""
批量读取简历文本：支持 .doc / .docx / .pdf 格式
输入：刘阳简历.doc、王向男-工作15年-【脉脉招聘】.pdf
输出：干净纯文本内容
"""

import os
import subprocess
import docx
import PyPDF2

def read_doc_by_textutil(file_path: str) -> str:
    try:
        result = subprocess.run(
            ["textutil", "-convert", "txt", "-stdout", file_path],
            check=True,
            capture_output=True
        )
        text = result.stdout.decode("utf-8", errors="ignore")
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        return "\n".join(lines)
    except Exception as e:
        return f"【Word 读取失败】{file_path}：{str(e)}"

def read_word(file_path: str) -> str:
    """读取 Word 文档（.doc/.docx）"""
    if file_path.lower().endswith(".doc"):
        return read_doc_by_textutil(file_path)

    try:
        doc = docx.Document(file_path)
        lines = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
        return "\n".join(lines)
    except Exception as e:
        fallback_text = read_doc_by_textutil(file_path)
        if fallback_text.startswith("【Word 读取失败】"):
            return f"【Word 读取失败】{file_path}：{str(e)}"
        return fallback_text

def read_pdf(file_path: str) -> str:
    """读取 PDF 文档"""
    try:
        with open(file_path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            pages = [page.extract_text().strip() for page in reader.pages]
        return "\n".join([p for p in pages if p])
    except Exception as e:
        return f"【PDF 读取失败】{file_path}：{str(e)}"

def read_resume(file_path: str) -> str:
    """自动判断格式并读取"""
    if not os.path.isabs(file_path):
        local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_path)
        if os.path.exists(local_path):
            file_path = local_path

    if not os.path.exists(file_path):
        return f"【文件不存在】{file_path}"
    
    ext = file_path.lower()
    if ext.endswith((".doc", ".docx")):
        return read_word(file_path)
    elif ext.endswith(".pdf"):
        return read_pdf(file_path)
    else:
        return f"【不支持格式】{file_path}"

# ==================== 主程序 ====================
if __name__ == "__main__":
    # 你的两个简历文件
    files = [
        "./刘阳简历.doc",
        "./王向男-工作15年-【脉脉招聘】.pdf"
    ]

    # 读取并打印
    for f in files:
        print("=" * 60)
        print(f"📄 文件名：{f}")
        print("=" * 60)
        text = read_resume(f)
        print(text)
        print("\n\n")
