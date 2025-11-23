import os
import base64
import requests
import json
from PIL import Image
import time
import logging
from pdf2image import convert_from_path
from typing import Dict, List, Union, Optional
import re
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# 配置信息
API_KEY = "2faa9d76f3d6462f9fafc0828ffdb7f7.ykKFOC4vSQrdQICS"
API_URL = "https://open.bigmodel.cn/api/paas/v4/chat/completions"

def convert_chinese_amount_to_number(chinese_amount):
    """将中文大写金额转换为数字

    Args:
        chinese_amount: 中文大写金额字符串

    Returns:
        转换后的数字
    """
    if not chinese_amount or not isinstance(chinese_amount, str):
        return 0

    # 中文数字映射
    chinese_digits = {
        '零': 0, '〇': 0, '一': 1, '二': 2, '三': 3, '四': 4,
        '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
        '壹': 1, '贰': 2, '叁': 3, '肆': 4, '伍': 5, '陆': 6,
        '柒': 7, '捌': 8, '玖': 9, '拾': 10, '佰': 100, '仟': 1000,
        '萬': 10000, '万': 10000, '億': 100000000, '亿': 100000000
    }

    # 单位映射
    unit_digits = {
        '元': 1, '圆': 1, '角': 0.1, '分': 0.01
    }

    try:
        # 如果已经是数字，直接返回
        if re.match(r'^-?\d+(\.\d+)?$', chinese_amount):
            return float(chinese_amount)

        # 处理中文大写金额
        amount = 0
        current_number = 0
        current_unit = 1
        decimal_part = 0
        in_decimal = False
        decimal_multiplier = 0.1

        for char in chinese_amount:
            if char in chinese_digits:
                if in_decimal:
                    decimal_part += chinese_digits[char] * decimal_multiplier
                    decimal_multiplier *= 0.1
                else:
                    if chinese_digits[char] < 10:
                        current_number = chinese_digits[char]
                    else:  # 十、百、千等单位
                        if current_number == 0:
                            current_number = 1
                        amount += current_number * chinese_digits[char]
                        current_number = 0
            elif char in unit_digits:
                if char in ['元', '圆']:
                    amount += current_number
                    current_number = 0
                    in_decimal = True
                elif char == '角':
                    decimal_part += current_number * 0.1
                    current_number = 0
                elif char == '分':
                    decimal_part += current_number * 0.01
                    current_number = 0
            elif char in ['整', '正']:
                # 结束符，忽略
                pass
            else:
                # 其他字符，忽略
                pass

        # 处理最后可能剩余的数字
        if current_number > 0:
            if in_decimal:
                decimal_part += current_number * decimal_multiplier
            else:
                amount += current_number

        total = amount + decimal_part
        return round(total, 2)

    except Exception as e:
        logging.warning(f"中文金额转换失败: {chinese_amount}, 错误: {str(e)}")
        return 0


def clean_amount_string(amount_str):
    """清理金额字符串，移除人民币符号和逗号，转换为数字

    支持中文大写金额转换
    """
    if not amount_str or amount_str == '':
        return 0

    if isinstance(amount_str, (int, float)):
        return amount_str

    # 如果是列表，取第一个元素
    if isinstance(amount_str, list):
        if amount_str:
            amount_str = amount_str[0]
        else:
            return 0

    try:
        # 检查是否是中文大写金额
        if any(char in amount_str for char in
               ['壹', '贰', '叁', '肆', '伍', '陆', '柒', '捌', '玖', '拾', '佰', '仟', '万', '元', '角', '分']):
            return convert_chinese_amount_to_number(amount_str)

        # 移除人民币符号、逗号和其他非数字字符（除了小数点和负号）
        cleaned = re.sub(r'[^\d.-]', '', str(amount_str))
        if cleaned == '' or cleaned == '-':
            return 0
        return float(cleaned)
    except (ValueError, TypeError):
        return 0

def format_project_name(project_name):
    """格式化项目名称字段，处理各种可能的类型

    Args:
        project_name: 项目名称字段，可能是字符串、列表、字典等

    Returns:
        格式化后的项目名称字符串
    """
    if not project_name:
        return ""

    # 如果是字符串，直接返回
    if isinstance(project_name, str):
        return project_name

    # 如果是列表，处理每个元素
    if isinstance(project_name, list):
        formatted_items = []
        for item in project_name:
            if isinstance(item, str):
                formatted_items.append(item)
            elif isinstance(item, dict):
                # 如果是字典，尝试提取有意义的值
                if '项目名称' in item:
                    formatted_items.append(str(item['项目名称']))
                else:
                    # 将字典的所有值连接起来
                    values = [str(v) for v in item.values()]
                    formatted_items.append(', '.join(values))
            else:
                formatted_items.append(str(item))
        return ', '.join(formatted_items)

    # 如果是字典，尝试提取有意义的值
    if isinstance(project_name, dict):
        if '项目名称' in project_name:
            return str(project_name['项目名称'])
        else:
            # 将字典的所有值连接起来
            values = [str(v) for v in project_name.values()]
            return ', '.join(values)

    # 其他类型，转换为字符串
    return str(project_name)

class InvoiceRecognizer:
    def __init__(self, api_key: str, api_url: str):
        """初始化票据识别器

        Args:
            api_key: 智谱AI API密钥
            api_url: API接口地址
        """
        self.api_key = api_key
        self.api_url = api_url
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}',
            'Accept': '*/*',
            'Host': 'open.bigmodel.cn',
            'Connection': 'keep-alive'
        }

    def _encode_image_to_base64(self, image_path: str) -> str:
        """将图片编码为Base64字符串 - 增强版本"""
        try:
            # 验证文件状态
            if not os.path.exists(image_path):
                raise FileNotFoundError(f"图片文件不存在: {image_path}")

            file_size = os.path.getsize(image_path)
            if file_size == 0:
                raise ValueError(f"图片文件为空: {image_path}")

            # 验证文件可读性
            if not os.access(image_path, os.R_OK):
                raise PermissionError(f"无法读取图片文件: {image_path}")

            with open(image_path, "rb") as image_file:
                image_data = image_file.read()

            if len(image_data) == 0:
                raise ValueError("读取的图片数据为空")

            encoded = base64.b64encode(image_data).decode('utf-8')
            logging.debug(f"图片编码成功: {image_path} -> {len(encoded)} 字符")
            return encoded

        except Exception as e:
            logging.error(f"图片编码失败: {str(e)}")
            raise

    def _convert_pdf_to_images(self, pdf_path: str) -> List[str]:
        """将PDF文件转换为图片 - 完全修复版本"""
        import tempfile
        temp_dir = None
        try:
            import shutil
            if not shutil.which('pdftoppm'):
                raise RuntimeError('请先安装poppler-utils并确保其在系统PATH中。')

            # 使用系统临时目录而不是原文件目录
            temp_dir = tempfile.mkdtemp(prefix="pdf_images_")
            logging.info(f"创建临时目录: {temp_dir}")

            # 转换PDF为图片
            images = convert_from_path(pdf_path)
            image_paths = []

            for i, image in enumerate(images):
                # 使用简单的文件名
                timestamp = int(time.time() * 1000)
                image_path = os.path.join(temp_dir, f'page_{i + 1}_{timestamp}.jpg')

                # 保存图片
                image.save(image_path, 'JPEG', quality=85)

                # 验证文件已创建
                if os.path.exists(image_path):
                    image_paths.append(image_path)
                    logging.info(f"PDF页面保存成功: {image_path}")
                else:
                    logging.error(f"PDF页面保存失败: {image_path}")

            if not image_paths:
                logging.error("PDF转换未生成任何图片文件")

            return image_paths

        except Exception as e:
            logging.error(f"PDF转换失败: {str(e)}")
            # 清理临时目录
            if temp_dir and os.path.exists(temp_dir):
                self._cleanup_temp_files([os.path.join(temp_dir, "*")])
            return []

    def _preprocess_image(self, image_path: str) -> str:
        """图像预处理 - 完全修复版本"""
        try:
            # 确保 image_path 是字符串且文件存在
            if isinstance(image_path, list):
                if image_path:
                    image_path = image_path[0]
                else:
                    raise ValueError("没有有效的图片路径")

            # 验证原始文件存在且可读
            if not os.path.exists(image_path):
                raise FileNotFoundError(f"原始图片文件不存在: {image_path}")

            if not os.access(image_path, os.R_OK):
                raise PermissionError(f"无法读取图片文件: {image_path}")

            logging.info(f"开始预处理图片: {image_path}")

            # 打开并处理图片
            try:
                image = Image.open(image_path)
                # 确保图片被加载到内存中
                image.load()
            except Exception as e:
                raise ValueError(f"无法打开图片文件: {str(e)}")

            processed_image = image.convert('RGB')

            # 创建唯一的输出文件名，但不在原临时目录中
            import tempfile
            timestamp = int(time.time() * 1000)
            base_name = os.path.basename(image_path)
            # 使用系统临时目录，而不是原PDF转换目录
            processed_path = os.path.join(tempfile.gettempdir(), f"processed_{base_name}_{timestamp}.jpg")

            # 确保目录存在
            os.makedirs(os.path.dirname(processed_path), exist_ok=True)

            # 保存处理后的图片
            processed_image.save(processed_path, 'JPEG', quality=90)

            # 验证保存的文件存在
            if not os.path.exists(processed_path):
                raise IOError(f"预处理后的文件保存失败: {processed_path}")

            logging.info(f"图像预处理完成: {image_path} -> {processed_path}")
            return processed_path

        except Exception as e:
            logging.error(f"图像预处理失败: {str(e)}")
            # 如果预处理失败，返回原路径（如果存在）
            if os.path.exists(image_path):
                logging.info("预处理失败，使用原始图片")
                return image_path
            else:
                raise

    def _cleanup_temp_files(self, image_paths: List[str]):
        """安全清理临时文件 - 增强版本"""
        if not image_paths:
            return

        try:
            # 获取所有要删除的文件
            files_to_delete = []
            dirs_to_delete = set()

            for img_path in image_paths:
                if os.path.exists(img_path):
                    files_to_delete.append(img_path)
                    dir_path = os.path.dirname(img_path)
                    if dir_path and os.path.exists(dir_path):
                        dirs_to_delete.add(dir_path)

            # 先删除文件
            for file_path in files_to_delete:
                try:
                    os.remove(file_path)
                    logging.debug(f"已删除临时文件: {file_path}")
                except Exception as e:
                    logging.warning(f"删除临时文件失败 {file_path}: {str(e)}")

            # 然后尝试删除目录（如果为空）
            for dir_path in dirs_to_delete:
                try:
                    # 检查目录是否为空
                    if not os.listdir(dir_path):
                        os.rmdir(dir_path)
                        logging.debug(f"已删除空临时目录: {dir_path}")
                    else:
                        logging.debug(f"临时目录不为空，跳过删除: {dir_path}")
                except Exception as e:
                    logging.debug(f"删除临时目录失败 {dir_path}: {str(e)}")

        except Exception as e:
            logging.warning(f"清理临时文件时出错: {str(e)}")

    def recognize_file(self, file_path: str, filename: str = "") -> Dict:
        """识别文件（支持图片和PDF）- 修复版本"""
        try:
            logging.info(f"=== 开始处理文件 ===")
            logging.info(f"文件路径: {file_path}")
            logging.info(f"文件名: {filename}")

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"文件不存在: {file_path}")

            file_ext = os.path.splitext(file_path)[1].lower()
            logging.info(f"文件扩展名: {file_ext}")

            if file_ext == '.pdf':
                logging.info("处理PDF文件")
                image_paths = self._convert_pdf_to_images(file_path)
                logging.info(f"PDF转换结果: {len(image_paths)} 张图片")

                if image_paths and len(image_paths) > 0:
                    # 使用第一张图片
                    image_path = image_paths[0]
                    logging.info(f"使用PDF第一页进行识别: {image_path}")
                    result = self._recognize_image(image_path)
                    result['source_type'] = 'pdf'
                    result['filename'] = filename

                    # 清理临时文件
                    self._cleanup_temp_files(image_paths)
                else:
                    return {"error": "PDF转换失败，没有生成图片"}

            elif file_ext in ['.jpg', '.jpeg', '.png', '.bmp']:
                logging.info("处理图片文件")
                result = self._recognize_image(file_path)
                result['source_type'] = 'image'
                result['filename'] = filename
            else:
                raise ValueError(f"不支持的文件类型: {file_ext}")

            # 后处理结果
            result = self._post_process_result(result)
            logging.info(f"=== 文件处理完成 ===")
            logging.info(f"结果: {result.get('tickets', [{}])[0].get('票据类型', '未知')}")

            return result

        except Exception as e:
            logging.error(f"文件识别失败: {str(e)}")
            return {"error": str(e), "file_path": file_path}

    def _extract_info_from_text(self, text: str) -> Dict:
        """当JSON解析失败时，从文本中提取关键信息"""
        try:
            # 初始化默认结果
            result = {
                "total_amount": 0,
                "tax_amount": 0,
                "tickets": [{
                    "票据类型": "",
                    "开票日期": "",
                    "价税合计": "",
                    "发票号码": "",
                    "项目名称": "",
                    "购买方": {"公司名称": "", "统一社会信用代码/纳税人识别号": ""},
                    "销售方": {"公司名称": "", "统一社会信用代码/纳税人识别号": ""}
                }]
            }

            # 尝试从文本中提取金额信息
            amount_patterns = [
                r'"total_amount":\s*([\d.]+)',
                r'总金额[：:]\s*([\d.]+)',
                r'金额[：:]\s*([\d.]+)',
                r'￥\s*([\d.]+)',
                r'¥\s*([\d.]+)'
            ]

            tax_patterns = [
                r'"tax_amount":\s*([\d.]+)',
                r'税额[：:]\s*([\d.]+)',
                r'进项税额[：:]\s*([\d.]+)'
            ]

            invoice_type_patterns = [
                r'"票据类型":\s*"([^"]*)"',
                r'票据类型[：:]\s*([^\s,]+)'
            ]

            project_name_patterns = [
                r'"项目名称":\s*"([^"]*)"',
                r'项目名称[：:]\s*([^\n]+)'
            ]

            # 提取总金额
            for pattern in amount_patterns:
                match = re.search(pattern, text)
                if match:
                    try:
                        result["total_amount"] = float(match.group(1))
                        break
                    except ValueError:
                        continue

            # 提取税额
            for pattern in tax_patterns:
                match = re.search(pattern, text)
                if match:
                    try:
                        result["tax_amount"] = float(match.group(1))
                        break
                    except ValueError:
                        continue

            # 提取票据类型
            for pattern in invoice_type_patterns:
                match = re.search(pattern, text)
                if match:
                    result["tickets"][0]["票据类型"] = match.group(1)
                    break

            # 提取项目名称
            for pattern in project_name_patterns:
                match = re.search(pattern, text)
                if match:
                    result["tickets"][0]["项目名称"] = match.group(1)
                    break

            # 如果能够提取到关键信息，添加成功标记
            if result["total_amount"] > 0 or result["tax_amount"] > 0:
                result["extracted_from_text"] = True
                logging.info("从文本中成功提取票据信息")

            return result

        except Exception as e:
            logging.error(f"从文本提取信息失败: {str(e)}")
            return {}

    def _recognize_image(self, image_path: str) -> Dict:
        """识别单个图片 - 完全修复版本"""
        processed_image_path = None
        start_time = time.time()

        try:
            # 确保 image_path 是有效的字符串路径
            if isinstance(image_path, list):
                if image_path:
                    image_path = image_path[0]
                else:
                    return {"error": "没有有效的图片路径"}

            # 详细验证文件状态
            if not os.path.exists(image_path):
                return {"error": f"图片文件不存在: {image_path}"}

            file_size = os.path.getsize(image_path)
            if file_size == 0:
                return {"error": f"图片文件为空: {image_path}"}

            logging.info(f"开始识别图片: {image_path} (大小: {file_size} bytes)")

            # 图像预处理 - 现在更健壮
            try:
                processed_image_path = self._preprocess_image(image_path)
                if not os.path.exists(processed_image_path):
                    return {"error": f"预处理后的图片文件不存在: {processed_image_path}"}
            except Exception as e:
                logging.error(f"图像预处理失败，使用原始图片: {str(e)}")
                processed_image_path = image_path  # 使用原始图片作为后备

            # 编码图片
            try:
                base64_image = self._encode_image_to_base64(processed_image_path)
                logging.info(f"图片Base64编码完成，数据长度: {len(base64_image)}")
            except Exception as e:
                return {"error": f"图片编码失败: {str(e)}"}

            # 构造API请求 - 使用更明确的提示词
            prompt = (
                "请从票据图片中提取关键信息，并严格遵守以下规则：\n"
                "1. 首先准确判断票据类型，特别注意区分：\n"
                "   - 增值税电子专用发票\n"
                "   - 增值税电子普通发票\n"
                "   - 其他票据类型\n"
                "2. 必须提取以下所有字段：\n"
                "   - 票据类型\n"
                "   - 开票日期\n"
                "   - 价税合计\n"
                "   - 发票号码\n"
                "   - 项目名称\n"
                "   - 购买方信息（公司名称、纳税人识别号）\n"
                "   - 销售方信息（公司名称、纳税人识别号）\n"
                "3. 特别注意：不同发票的发票号码、开票日期、金额等信息必须不同\n"
                "4. 如果图片不是票据，请返回错误信息\n\n"
                "## 金额字段提取规则：\n"
                "1. **价税合计**：必须提取发票上明确标注的\"价税合计\"或\"合计\"金额\n"
                "2. **税额**：必须提取发票上明确标注的\"税额\"金额"
                "3. **不含税金额**：如果发票上有\"金额\"或\"不含税金额\"字段，提取该值"
                "要求以JSON格式输出：{\n"
                '  "total_amount": 总金额(数字),\n'
                '  "tax_amount": 进项税金额(数字),\n'
                '  "tickets": [{\n'
                '    "票据类型": "",\n'
                '    "开票日期": "",\n'
                '    "价税合计": "",\n'
                '    "发票号码": "",\n'
                '    "项目名称": "",\n'
                '    "购买方": {"公司名称": "", "统一社会信用代码/纳税人识别号": ""},\n'
                '    "销售方": {"公司名称": "", "统一社会信用代码/纳税人识别号": ""}\n'
                '  }]\n'
                "}"
            )

            payload = json.dumps({
                "model": "glm-4v-flash",
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_image}"
                                }
                            }
                        ]
                    }
                ]
            })

            # 发送API请求
            logging.info("发送API请求...")
            response = requests.post(self.api_url, headers=self.headers, data=payload, timeout=60)
            response.raise_for_status()

            # 解析响应
            result = response.json()
            content = result['choices'][0]['message']['content']

            # 记录原始响应用于调试
            logging.info(f"API原始响应: {content[:200]}...")
            # 清理JSON字符串
            content = content.strip()
            json_start = content.find('{')
            json_end = content.rfind('}') + 1

            if json_start >= 0 and json_end > json_start:
                json_str = content[json_start:json_end]
            else:
                json_str = content

            json_str = json_str.replace('```json', '').replace('```', '').strip()

            # 尝试解析JSON
            try:
                parsed_result = json.loads(json_str)
                logging.info(f"JSON解析成功: {parsed_result}")
            except json.JSONDecodeError as e:
                logging.warning(f"JSON解析失败，尝试修复: {str(e)}")
                try:
                    json_str = self._fix_json_format(json_str)
                    parsed_result = json.loads(json_str)
                    logging.info(f"JSON修复成功: {parsed_result}")
                except json.JSONDecodeError as e2:
                    logging.error(f"JSON修复失败: {str(e2)}")
                    parsed_result = self._extract_info_from_text(content)
                    if not parsed_result:
                        return {"error": f"JSON解析失败: {str(e2)}", "raw_content": content}

            # 添加处理时间
            parsed_result['processing_time'] = time.time() - start_time
            parsed_result['source_image'] = image_path  # 添加源文件信息用于调试

            # 解析JSON后添加调试
            if 'tickets' in parsed_result and parsed_result['tickets']:
                ticket = parsed_result['tickets'][0]
                logging.info(f"解析后的金额字段:")
                logging.info(f"  total_amount: {parsed_result.get('total_amount')}")
                logging.info(f"  tax_amount: {parsed_result.get('tax_amount')}")
                logging.info(f"  价税合计: {ticket.get('价税合计')}")

                # 检查一致性
                total = parsed_result.get('total_amount', 0)
                tax_total_clean = clean_amount_string(ticket.get('价税合计', 0))

                if abs(total - tax_total_clean) > 0.01:
                    logging.warning(f"识别结果不一致: total_amount({total}) ≠ 价税合计({tax_total_clean})")

            return parsed_result

        except Exception as e:
            logging.error(f"图片识别失败: {str(e)}")
            return {"error": str(e)}
        finally:
            # 更安全的临时文件清理
            if (processed_image_path and
                    processed_image_path != image_path and  # 不是原始文件
                    os.path.exists(processed_image_path)):
                try:
                    os.remove(processed_image_path)
                    logging.debug(f"已删除临时预处理文件: {processed_image_path}")
                except Exception as e:
                    logging.warning(f"删除临时预处理文件失败: {str(e)}")

    def _fix_json_format(self, json_str: str) -> str:
        """尝试修复常见的JSON格式问题"""
        try:
            # 1. 修复未闭合的字符串
            json_str = re.sub(r',\s*}', '}', json_str)  # 移除对象末尾多余的逗号
            json_str = re.sub(r',\s*]', ']', json_str)  # 移除数组末尾多余的逗号

            # 2. 修复单引号问题（将单引号替换为双引号）
            # 但要注意不要替换已经转义的单引号
            json_str = re.sub(r"(?<!\\)'", '"', json_str)

            # 3. 修复未转义的特殊字符
            json_str = json_str.replace('\\', '\\\\')
            json_str = json_str.replace('\n', '\\n')
            json_str = json_str.replace('\t', '\\t')
            json_str = json_str.replace('\r', '\\r')

            # 4. 修复属性名缺少引号的问题
            # 匹配模式：属性名: 值
            json_str = re.sub(r'(\w+)\s*:', r'"\1":', json_str)

            # 5. 修复未闭合的引号
            lines = json_str.split('\n')
            fixed_lines = []
            for line in lines:
                # 计算一行中引号的数量
                quote_count = line.count('"')
                if quote_count % 2 != 0:
                    # 如果引号数量是奇数，可能需要修复
                    if line.count('":') > 0 and line.count('"') == 1:
                        # 可能是键没有闭合的引号
                        line = line.replace('":', '"":')
                fixed_lines.append(line)

            return '\n'.join(fixed_lines)

        except Exception as e:
            logging.error(f"JSON修复过程中出错: {str(e)}")
            return json_str

    def _determine_tax_type(self, invoice_type: str, project_name: str = "", tax_amount: float = 0,
                            seller_name: str = "", filename: str = "") -> str:
        """根据发票类型和项目名称确定进项税类型"""
        # 确保invoice_type是字符串
        if not isinstance(invoice_type, str):
            invoice_type = str(invoice_type) if invoice_type else ""

        invoice_type_lower = invoice_type.lower()

        # 1. 处理酒店结账单
        if "酒店结账单" in invoice_type:
            return "非发票"

        # 2. 如果税额为0，直接返回"无"
        if tax_amount == 0:
            return "无"

        # 3. 增值税电子专用发票
        if ("增值税专用发票" in invoice_type or
                "增值税电子专用发票" in invoice_type_lower or
                "专用发票" in invoice_type):
            return "勾选抵扣"

        # 4. 增值税电子普通发票 - 修复逻辑
        elif ("增值税电子普通发票" in invoice_type_lower or
              "电子发票（普通发票）" in invoice_type_lower or
              "电子发票" in invoice_type_lower):

            # 处理项目名称，确保是字符串格式
            project_name_str = format_project_name(project_name)
            project_name_lower = project_name_str.lower()
            print("$$$$$$$$$$$$$$", invoice_type)
            # 增值税电子普通发票的判定
            if ("增值税电子普通发票" in invoice_type_lower or
                    "电子发票（普通发票）" in invoice_type_lower or
                    "电子发票" == invoice_type):  # 增加对"电子发票"类型的直接判断

                # 检查项目名称是否包含"运输服务"
                # 放宽匹配条件，只要包含"运输服务"即认为符合条件
                if "运输服务" in project_name_lower:
                    return "计算抵扣运输服务"  # 根据政策，此处应为计算抵扣
                # 其他情况（如通行费）的判断...
                elif "通行费" in project_name_lower:
                    return "勾选抵扣"
                else:
                    # 对于项目名称不明确的情况，可能需要更复杂的逻辑或默认处理
                    # 目前先返回"无"
                    return "无"

            # 特殊处理：滴滴出行发票
            is_didi_invoice = ("滴滴" in seller_name and "科技" in seller_name) or (
                    "滴滴" in filename and "电子发票" in filename)

            if is_didi_invoice:
                return "计算抵扣运输服务"

            # 只有当项目名称明确包含"运输服务"时，才返回"计算抵扣运输服务"
            elif "运输服务" in project_name_lower:
                return "计算抵扣运输服务"

            # 只有当项目名称明确包含"通行费"时，才返回"勾选抵扣"
            elif "通行费" in project_name_lower:
                return "勾选抵扣"

            # 其他情况，税额应该已经被设为0，所以会返回"无"
            else:
                return "无"

        # 5. 铁路电子客票
        elif "铁路电子客票" in invoice_type_lower or "电子客票" in invoice_type_lower:
            return "勾选抵扣"

        # 6. 航空运输电子客票行程单
        elif "航空运输电子客票行程单" in invoice_type_lower or "电子客票行程单" in invoice_type_lower:
            return "勾选抵扣"

        # 7. 飞机票纸质行程单
        elif "飞机票纸质行程单" in invoice_type_lower or "纸质行程单" in invoice_type_lower:
            return "计算抵扣运输服务"

        # 8. 公路、水路等其他纸质客票
        elif "公路" in invoice_type_lower or "水路" in invoice_type_lower or "客票" in invoice_type_lower:
            return "计算抵扣运输服务"

        # 默认情况
        else:
            return "未知类型"

    def _post_process_result(self, result: Dict) -> Dict:
        """对识别结果进行后处理 - 确保total_amount等于价税合计"""
        if 'error' in result:
            return result

        if not result.get('tickets') or len(result['tickets']) == 0:
            result['error'] = "未识别到任何票据信息"
            return result

        ticket = result['tickets'][0]

        # 清理金额字段
        original_total = clean_amount_string(result.get('total_amount', 0))
        original_tax = clean_amount_string(result.get('tax_amount', 0))
        original_tax_total = clean_amount_string(ticket.get('价税合计', 0))

        logging.info(
            f"原始金额 - total_amount: {original_total}, tax_amount: {original_tax}, 价税合计: {original_tax_total}")

        # 核心修复：确保total_amount等于价税合计
        # 优先使用价税合计字段的值
        if original_tax_total > 0:
            # 如果total_amount与价税合计不一致，以价税合计为准
            if abs(original_total - original_tax_total) > 0.01:
                logging.info(f"修正total_amount: {original_total} -> {original_tax_total} (使用价税合计的值)")
                result['total_amount'] = original_tax_total
            else:
                result['total_amount'] = original_tax_total
        else:
            # 如果价税合计为0但total_amount不为0，使用total_amount作为价税合计
            if original_total > 0:
                ticket['价税合计'] = original_total
                logging.info(f"价税合计为空，使用total_amount: {original_total}")
            result['total_amount'] = original_total

        # 确保tax_amount合理
        final_total = result['total_amount']
        final_tax = original_tax

        # 税额不能大于价税合计
        if final_tax > final_total:
            logging.warning(f"税额({final_tax})大于价税合计({final_total})，设为0")
            result['tax_amount'] = 0
        else:
            result['tax_amount'] = final_tax

        # 更新ticket中的价税合计字段
        ticket['价税合计'] = final_total

        logging.info(
            f"最终金额 - total_amount: {result['total_amount']}, tax_amount: {result['tax_amount']}, 价税合计: {ticket['价税合计']}")

        # 其余业务逻辑保持不变
        invoice_type = ticket.get('票据类型', '')
        project_name = ticket.get('项目名称', '')
        seller_info = ticket.get('销售方', {})
        seller_name = seller_info.get('公司名称', '') if isinstance(seller_info, dict) else ''
        filename = result.get('filename', '')

        # 处理酒店结账单
        if "酒店结账单" in invoice_type:
            result['tax_amount'] = 0
            result['tax_type'] = "非发票"
            return result

        # 处理铁路电子客票等特殊逻辑
        if invoice_type in ['铁路电子客票', '电子客票']:
            if final_total > 0:
                # 铁路电子客票的税额计算
                result['tax_amount'] = round(final_total / 1.09 * 0.09, 2)
                logging.info(f"铁路电子客票计算税额: {result['tax_amount']}")

        # 处理电子普通发票的特殊逻辑
        elif invoice_type in ['电子发票（普通发票）', '增值税电子普通发票', '电子发票']:
            is_didi_invoice = ("滴滴" in seller_name and "科技" in seller_name) or (
                    "滴滴" in filename and "电子发票" in filename)

            if not is_didi_invoice:
                project_names = []
                if isinstance(project_name, list):
                    project_names = project_name
                elif isinstance(project_name, str):
                    project_names = [project_name]
                else:
                    project_names = [str(project_name)]

                has_transport_or_toll = False
                for proj in project_names:
                    proj_str = str(proj)
                    if any(keyword in proj_str for keyword in ['运输服务', '通行费', '*运输服务*']):
                        has_transport_or_toll = True
                        break

                if not has_transport_or_toll:
                    logging.info("电子普通发票项目名称不包含运输服务或通行费，将税额设为0")
                    result['tax_amount'] = 0

        # 确定进项税类型
        result['tax_type'] = self._determine_tax_type(invoice_type, project_name, result.get('tax_amount', 0),
                                                      seller_name, filename)

        return result
