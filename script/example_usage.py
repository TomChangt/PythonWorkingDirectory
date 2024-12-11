from excel_to_json_converter import ExcelToJsonConverter


def main():
    # 创建转换器实例
    converter = ExcelToJsonConverter("/Users/changtong/Downloads/CNC工时.xlsx")

    # 获取所有工作表名称
    sheet_names = converter.get_sheet_names()
    print(f"Excel文件中的工作表: {sheet_names}")

    # 定义要排除的字段
    exclude_fields = ["序号", "调机时间", "备注"]

    # 将第一个工作表转换为JSON并保存，排除指定字段
    converter.save_to_json_file(
        output_file="CNC工时.json",
        sheet_name=0,  # 可以使用索引或工作表名称
        ensure_ascii=False,  # 支持中文输出
        indent=2,  # 格式化JSON输出
        exclude_fields=exclude_fields,  # 排除指定字段
    )

    # 获取JSON数据但不保存文件，同样排除指定字段
    json_data = converter.convert_sheet_to_json(
        sheet_name=0, exclude_fields=exclude_fields
    )
    print("转换后的数据:", json_data)


if __name__ == "__main__":
    main()
