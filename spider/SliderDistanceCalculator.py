import cv2
import numpy as np


class SliderDistanceCalculator:
    def __init__(self):
        self.min_threshold = 100
        self.max_threshold = 200
        self.match_method = cv2.TM_CCOEFF_NORMED

    def preprocess_image(self, image):
        """预处理图像：转灰度、高斯模糊、边缘检测"""
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        blurred = cv2.GaussianBlur(gray, (5, 5), 0)
        edges = cv2.Canny(blurred, self.min_threshold, self.max_threshold)
        return edges

    def find_slider_position(self, bg_image, slider_image):
        """找到滑块在背景图中的初始位置"""
        bg_edges = self.preprocess_image(bg_image)
        slider_edges = self.preprocess_image(slider_image)

        result = cv2.matchTemplate(bg_edges, slider_edges, self.match_method)
        _, _, _, max_loc = cv2.minMaxLoc(result)

        return max_loc

    def calculate_distance(self, bg_image_path, slider_image_path):
        """计算滑块需要移动的距离"""
        bg_image = cv2.imread(bg_image_path)
        slider_image = cv2.imread(slider_image_path)

        # 找到滑块在背景图中的初始位置
        initial_pos = self.find_slider_position(bg_image, slider_image)

        # 在背景图中裁剪出与滑块大小相同的区域进行比较
        bg_edges = self.preprocess_image(bg_image)
        slider_height, slider_width = slider_image.shape[:2]
        search_area = bg_edges[initial_pos[1] :, initial_pos[0] + slider_width :]

        # 在搜索区域中寻找滑块应该滑动到的位置
        slider_edges = self.preprocess_image(slider_image)
        result = cv2.matchTemplate(search_area, slider_edges, self.match_method)
        _, _, _, max_loc = cv2.minMaxLoc(result)

        # 计算滑动距离
        slide_distance = max_loc[0] + slider_width

        # 添加额外的验证步骤
        if self.verify_distance(bg_image, slider_image, initial_pos, slide_distance):
            return slide_distance, initial_pos
        else:
            print("警告：计算出的距离可能不准确，需要进一步验证。")
            return None, initial_pos

    def verify_distance(self, bg_image, slider_image, initial_pos, distance):
        """验证计算出的距离是否合理"""
        # 在这里添加额外的验证逻辑
        # 例如：检查滑块移动后的位置是否与背景图中的缺口匹配
        # 或者使用多种方法计算距离，取平均值或中位数
        # 如果验证通过，返回True；否则返回False
        # 这里只是一个示例，您需要根据实际情况实现更复杂的验证逻辑
        bg_height, bg_width = bg_image.shape[:2]
        if distance < 0 or distance > bg_width - initial_pos[0]:
            return False
        return True

    def visualize_result(self, bg_image_path, slider_image_path, distance, initial_pos):
        """可视化结果（用于调试）"""
        bg_image = cv2.imread(bg_image_path)
        slider_image = cv2.imread(slider_image_path)

        # 在背景图上画出滑块的初始位置
        cv2.rectangle(
            bg_image,
            initial_pos,
            (
                initial_pos[0] + slider_image.shape[1],
                initial_pos[1] + slider_image.shape[0],
            ),
            (0, 255, 0),
            2,
        )

        # 在背景图上画一条竖线表示计算出的距离
        if distance is not None:
            final_pos = (initial_pos[0] + distance, initial_pos[1])
            cv2.line(
                bg_image,
                final_pos,
                (final_pos[0], final_pos[1] + slider_image.shape[0]),
                (0, 0, 255),
                2,
            )

        # 显示结果
        cv2.imshow("Background with Initial and Final Positions", bg_image)
        cv2.imshow("Slider", slider_image)
        cv2.waitKey(0)
        cv2.destroyAllWindows()


# 使用示例
if __name__ == "__main__":
    calculator = SliderDistanceCalculator()
    bg_path = "./data/siideBg.png"
    slider_path = "./data/siide.png"

    try:
        distance, initial_pos = calculator.calculate_distance(bg_path, slider_path)
        print(f"滑块的初始位置: {initial_pos}")
        if distance is not None:
            print(f"计算出的滑动距离: {distance}像素")
        else:
            print("无法准确计算滑动距离")

        # 可视化结果（可选）
        calculator.visualize_result(bg_path, slider_path, distance, initial_pos)
    except Exception as e:
        print(f"计算过程中出错: {str(e)}")
