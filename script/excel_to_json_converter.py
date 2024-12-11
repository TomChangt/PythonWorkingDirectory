import pandas as pd
import json
from typing import Union, Dict, List
from pathlib import Path


class ExcelToJsonConverter:
    def __init__(self, excel_file: Union[str, Path]):
        """
        初始化转换器
        :param excel_file: Excel 文件路径
        """
        self.excel_file = Path(excel_file)
        if not self.excel_file.exists():
            raise FileNotFoundError(f"Excel文件不存在: {excel_file}")

    def convert_sheet_to_json(
        self, sheet_name: Union[str, int] = 0, exclude_fields: List[str] = None
    ) -> List[Dict]:
        """
        将指定的工作表转换为JSON格式
        :param sheet_name: 工作表名称或索引（默认为第一个工作表）
        :param exclude_fields: 需要排除的字段列表
        :return: JSON格式的数据列表
        """
        try:
            # 读取Excel文件
            df = pd.read_excel(self.excel_file, sheet_name=sheet_name)

            # 如果有需要排除的字段，则从DataFrame中删除这些列
            if exclude_fields:
                # 过滤掉不存在的列名，避免报错
                valid_excludes = [col for col in exclude_fields if col in df.columns]
                if valid_excludes:
                    df = df.drop(columns=valid_excludes)

                # 如果有无效的列名，打印警告信息
                invalid_excludes = set(exclude_fields) - set(valid_excludes)
                if invalid_excludes:
                    print(f"警告: 以下字段在Excel中不存在: {list(invalid_excludes)}")

            # 将DataFrame转换为JSON格式
            json_data = df.to_dict(orient="records")
            return json_data
        except Exception as e:
            raise Exception(f"转换过程中出错: {str(e)}")

    def save_to_json_file(
        self,
        output_file: Union[str, Path],
        sheet_name: Union[str, int] = 0,
        ensure_ascii: bool = False,
        indent: int = 2,
        exclude_fields: List[str] = None,
    ) -> None:
        """
        将Excel数据转换并保存为JSON文件
        :param output_file: 输出JSON文件路径
        :param sheet_name: 工作表名称或索引
        :param ensure_ascii: 是否确保ASCII编码（默认False，支持中文）
        :param indent: JSON缩进空格数
        :param exclude_fields: 需要排除的字段列表
        """
        try:
            json_data = self.convert_sheet_to_json(
                sheet_name=sheet_name, exclude_fields=exclude_fields
            )
            output_path = Path(output_file)

            # 确保输出目录存在
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # 写入JSON文件
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(
                    json_data, f, ensure_ascii=ensure_ascii, indent=indent, default=str
                )

            print(f"成功将Excel转换为JSON文件: {output_path}")

        except Exception as e:
            raise Exception(f"保存JSON文件时出错: {str(e)}")

    def get_sheet_names(self) -> List[str]:
        """
        获取Excel文件中所有工作表的名称
        :return: 工作表名称列表
        """
        try:
            excel_file = pd.ExcelFile(self.excel_file)
            return excel_file.sheet_names
        except Exception as e:
            raise Exception(f"获取工作表名称时出错: {str(e)}")
