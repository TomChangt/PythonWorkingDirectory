import time
import os
import numpy as np
from random import uniform, randint
import pyautogui
import cv2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
from SliderDistanceCalculator import SliderDistanceCalculator

# 加载环境变量
load_dotenv()


# 加载环境变量
def bezier_curve(t):
    return t * t * (3.0 - 2.0 * t)


def simulate_human_drag(distance):
    print(f"开始移动滑动验证码, 距离: {distance}")
    loc = pyautogui.locateCenterOnScreen("./data/siide.png", confidence=0.9)
    if loc is None:
        print("无法找到图像")
        return
    start_x, start_y = loc
    pyautogui.moveTo(start_x, start_y)
    time.sleep(uniform(1, 5))
    pyautogui.mouseDown()

    duration = uniform(0.5, 1.5)
    steps = int(duration * 60)  # 假设60fps

    for i in range(steps):
        t = i / steps
        ease = bezier_curve(t)
        x = int(start_x + distance * ease)
        y = int(start_y + randint(-2, 2) * (1 - ease))  # 添加一些垂直方向的随机移动
        pyautogui.moveTo(x, y, duration=1 / 60)

    # 模拟人类结束时的微调
    for _ in range(2):
        pyautogui.moveRel(randint(-3, 3), randint(-2, 2), duration=0.05)

    time.sleep(uniform(0.1, 0.3))
    pyautogui.mouseUp()


def get_slide_distance(bg_image: str, slider_image: str) -> int:
    calculator = SliderDistanceCalculator()
    distance, initial_pos = calculator.calculate_distance(bg_image, slider_image)

    if distance is None:
        print("无法准确计算滑动距离，使用备用方法")
        return get_slide_distance_backup(bg_image, slider_image)

    return distance


def get_slide_distance_backup(bg_image: str, slider_image: str) -> int:
    # 加载背景图像和滑块图像
    bg = cv2.imread(bg_image)
    slider = cv2.imread(slider_image)

    # 转换为灰度图
    bg_gray = cv2.cvtColor(bg, cv2.COLOR_BGR2GRAY)
    slider_gray = cv2.cvtColor(slider, cv2.COLOR_BGR2GRAY)

    # 使用模板匹配找到滑块位置
    result = cv2.matchTemplate(bg_gray, slider_gray, cv2.TM_CCOEFF_NORMED)
    _, _, _, max_loc = cv2.minMaxLoc(result)

    # 计算滑块需要滑动的距离
    slider_width = slider.shape[1]
    distance = max_loc[0] + slider_width / 2  # 滑块中心位置
    return distance


def move_slide_code(driver: webdriver.Chrome) -> None:
    WebDriverWait(driver, 10).until(
        EC.frame_to_be_available_and_switch_to_it((By.ID, "tcaptcha_iframe_dy"))
    )
    siideBg_dom = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.ID, "slideBg"))
    )
    siideBg_dom.screenshot("./data/siideBg.png")
    siideBg_dom_loc = siideBg_dom.location

    slide_dom_path = '//*[@id="tcOperation"]/div[7]'
    temp_dom_loc = driver.find_element(By.XPATH, slide_dom_path).location
    if temp_dom_loc["x"] == siideBg_dom_loc["x"]:
        slide_dom_path = '//*[@id="tcOperation"]/div[8]'
    slide_dom = driver.find_element(By.XPATH, slide_dom_path)
    slide_dom.screenshot("./data/siide.png")

    distance = get_slide_distance("./data/siideBg.png", "./data/siide.png")
    print(f"计算出的滑动距离: {distance}像素")

    simulate_human_drag(distance)

    time.sleep(uniform(1, 2))
    driver.switch_to.default_content()


def click_button(driver: webdriver.Chrome, fpath: str) -> None:
    button = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located(
            (By.XPATH, "/html/body/div[1]/div[2]/div[1]/div[5]/a")
        )
    )
    button.screenshot(fpath)

    loc = pyautogui.locateCenterOnScreen(fpath)
    if loc is None:
        print(f"无法找到图像: {fpath}")
        return
    pyautogui.moveTo(loc)
    time.sleep(uniform(0.5, 1.5))
    pyautogui.click()


def login(url: str, username: str, password: str) -> None:
    chrome_options = Options()
    chrome_options.debugger_address = "127.0.0.1:9222"

    service = Service(executable_path="/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)

    WebDriverWait(driver, 10).until(
        EC.frame_to_be_available_and_switch_to_it(
            (By.XPATH, '//*[@id="anony-reg-new"]/div/div[1]/iframe')
        )
    )

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[1]/ul[1]/li[2]"))
    ).click()

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "username"))
    ).send_keys(username)
    time.sleep(uniform(0.5, 1.5))

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "password"))
    ).send_keys(password)
    time.sleep(uniform(0.5, 1.5))

    click_button(driver, "./data/douban_login_btn.png")
    move_slide_code(driver)


if __name__ == "__main__":
    login(
        "https://www.douban.com/", os.getenv("DOUBAN_U_NAME"), os.getenv("DOUBAN_PWD")
    )
