import os
import json
import time
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, UploadFile, File, Response
from fastapi.openapi.utils import get_openapi
from openai import OpenAI
import pandas as pd
import asyncio
import logging
from bs4 import BeautifulSoup
from io import BytesIO

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 初始化 FastAPI 应用
app = FastAPI(
    title="Excel Translation API",
    description="API for translating specific fields in Excel files to German",
    version="1.0.0",
)


# 自定义 OpenAPI 模式
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Excel Translation API",
        version="1.0.0",
        description="API for translating specific fields in Excel files to German",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi

# 初始化 OpenAI 客户端
api_key = os.environ.get("OPENAI_API_KEY")
if not api_key:
    raise ValueError("��设置 OPENAI_API_KEY 环境变量")

os.environ["HTTPS_PROXY"] = "http://127.0.0.1:7897"
os.environ["HTTP_PROXY"] = "http://127.0.0.1:7897"

client = OpenAI(api_key=api_key)


async def translate_text(text: str, target_language: str, model: str) -> str:
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": """你是一位专精于工业计量和测量设备领域的德语翻译专家，具有以下专业特点：
1. 精通工业量具、计量器具和测量设备的专业术语
2. 熟悉德语工业标准(DIN)中的度量衡和计量相关术语
3. 深入理解各类量具的技术特性和应用场景

翻译要求：
1. 严格使用德语工业标准中规定的专业术语
2. 保持技术描述的精确性和专业性
3. 确保符合德语工业文档的表达习惯
4. 对于精密仪器的规格、参数等信息需要特别准确
5. 保持专业度量单位的标准表达方式

领域关键词参考：
- Messinstrumente（测量仪器）
- Messwerkzeuge（测量工具）
- Präzisionsmessgeräte（精密测量设备）
- Messmittel（量具）
- Messtechnik（测量技术）""",
                },
                {
                    "role": "user",
                    "content": f"请将以下工业量具相关的英文文本翻译成德语:\n\n{text}",
                },
            ],
            temperature=0.2,  # 降低温度以提高一致性
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"翻译时出错: {str(e)}")
        return f"TRANSLATION_ERROR: {str(e)}"


async def translate_html(html_content: str, target_language: str, model: str) -> str:
    soup = BeautifulSoup(html_content, "html.parser")
    for text in soup.find_all(text=True):
        if text.strip():
            translated_text = await translate_text(text.strip(), target_language, model)
            if not translated_text.startswith("TRANSLATION_ERROR"):
                text.replace_with(translated_text)
    return str(soup)


async def translate_json_field(json_str: str, target_language: str, model: str) -> str:
    try:
        data = json.loads(json_str)
        if "data" in data:
            data["data"] = await translate_html(data["data"], target_language, model)
        return json.dumps(data, ensure_ascii=False)
    except json.JSONDecodeError:
        logger.error("无效的 JSON 字符串")
        return json_str


async def process_excel(file: UploadFile, target_language: str, model: str) -> tuple:
    df = pd.read_excel(file.file)
    error_log = []

    if "原数据EN" not in df.columns:
        raise HTTPException(status_code=400, detail="Excel文件中未找到'原数据EN'列")

    translated_column = []
    for idx, text in enumerate(df["原数据EN"]):
        if pd.isna(text):  # 处理空值
            translated_column.append("")
            continue

        translated_text = await translate_text(str(text), target_language, model)
        if translated_text.startswith("TRANSLATION_ERROR"):
            error_log.append(f"Row: {idx+2}, Error: {translated_text}")
            translated_text = text  # 保留原文
        translated_column.append(translated_text)

    # 添加新的翻译列
    df["大模型翻译数据DE"] = translated_column

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return output, error_log


@app.post(
    "/translate-excel",
    summary="Translate Excel file",
    description="Translates '原数据EN' column to German and adds the translation as '大模型翻译数据DE' column.",
)
async def translate_excel(
    file: UploadFile = File(..., description="Excel file to be translated"),
    target_language: Optional[str] = "German",
    model: Optional[str] = "gpt-4",
):
    """
    Translates an Excel file's '原数据EN' column to German.

    - **file**: The Excel file to be translated
    - **target_language**: The target language for translation (default is German)
    - **model**: The GPT model to use for translation (default is gpt-4)

    This endpoint will:
    1. Read the '原数据EN' column from the Excel file
    2. Translate the content to German
    3. Add a new column '大模型翻译数据DE' with the translations
    4. Return the updated Excel file

    Raises:
    - **HTTPException 400**: If the uploaded file is not valid or missing required column
    - **HTTPException 500**: If there's an error during the translation process
    """
    start_time = time.time()
    try:
        translated_excel, error_log = await process_excel(file, target_language, model)
        end_time = time.time()
        process_time = end_time - start_time

        headers = {
            "Content-Disposition": f'attachment; filename="translated_excel.xlsx"',
            "X-Process-Time": f"{process_time:.2f} seconds",
        }

        if error_log:
            headers["X-Translation-Errors"] = "; ".join(
                error_log[:5]
            )  # 只返回前5个错误

        return Response(
            content=translated_excel.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )
    except Exception as e:
        logger.error(f"处理 Excel 文件时出错: {str(e)}")
        raise HTTPException(status_code=500, detail=f"处理 Excel 文件时出错: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
