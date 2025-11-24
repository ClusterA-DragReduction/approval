import streamlit as st
import requests
import json
import time
from dotenv import load_dotenv
import os
import logging
import pandas as pd
from io import BytesIO
import base64
import hmac
import hashlib
import urllib.parse
from datetime import datetime, timedelta
import re
import pymysql
from pymysql.cursors import DictCursor
import fitz  # PyMuPDF
from invoice_recognizer import InvoiceRecognizer
import invoice_recognizer
import tempfile
import shutil
import zipfile
import tarfile
from pathlib import Path
import uuid

# 加载环境变量
load_dotenv()

# 配置信息
CORP_ID = os.getenv('CORP_ID')
DING_APP_KEY = os.getenv('DING_APP_KEY')
DING_APP_SECRET = os.getenv('DING_APP_SECRET')
DING_REDIRECT_URI = os.getenv('DING_REDIRECT_URI', 'http://localhost:8501')
DING_AGENT_ID = os.getenv('DING_AGENT_ID')
DING_PROCESS_CODE = os.getenv('DING_PROCESS_CODE')
DING_PROCESS_CODE_MONEY = os.getenv("DING_PROCESS_CODE_MONEY")
DING_PROCESS_CODE_TRAVEL = os.getenv("DING_PROCESS_CODE_TRAVEL")
DING_PROCESS_CODE_RD = os.getenv("DING_PROCESS_CODE_RD")
DING_PROCESS_CODE_MARKET = os.getenv("DING_PROCESS_CODE_MARKET")
DING_PROCESS_CODE_EXPENSE_TYPE = os.getenv("DING_PROCESS_CODE_EXPENSE_TYPE")

# 数据库配置
DB_CONFIG = {
    'host': 'mysql2.sqlpub.com',
    'user': 'mems_root',
    'port': 3307,
    'password': 'Rv2XGAPhGRQwUKH7',
    'database': 'db_connection_2025',
    'charset': 'utf8mb4',
    'cursorclass': DictCursor
}

# 页面配置
st.set_page_config(
    page_title="智能票据审批系统",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded"
)


# 加载静态HTML内容
def load_static_content():
    """加载静态HTML内容"""
    return """
    <style>
        .main-header {
            font-size: 2.5rem;
            color: #1f77b4;
            text-align: center;
            margin-bottom: 2rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid #1f77b4;
        }
        .custom-subheader {
            font-size: 24px;
            color: #1f77b4;
            font-weight: bold;
            margin-bottom: 20px;
        }
        .custom-warning {
            background-color: #fff3cd;
            border: 1px solid #ffeaa7;
            color: #FF0000;
            padding: 10px;
            border-radius: 4px;
            font-size: 20px;
            font-family: 'Arial', sans-serif;
            margin-bottom: 1rem;
        }
        .info-box {
            background-color: #d1ecf1;
            color: #0c5460;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            border: 1px solid #bee5eb;
        }
        .success-box {
            background-color: #d4edda;
            color: #155724;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            border: 1px solid #c3e6cb;
        }
        .error-box {
            background-color: #f8d7da;
            color: #721c24;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            border: 1px solid #f5c6cb;
        }
    </style>
    """


# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# 钉钉审批类
class DingTalkApproval:
    def __init__(self):
        self.app_key = DING_APP_KEY
        self.app_secret = DING_APP_SECRET
        self.agent_id = DING_AGENT_ID

        # 使用登录用户信息
        if 'dingtalk_user' in st.session_state and st.session_state.dingtalk_user:
            self.dd_user_id = st.session_state.dingtalk_user.get('userid', '')
            self.union_id = st.session_state.dingtalk_user.get('unionId', '')
        else:
            self.dd_user_id = ''
            self.union_id = ''

        self.access_token = self.get_access_token()
        self.user_role = ''
        self.dept_id = self.get_user_current_department_id()
        self.dept_name = self.get_user_current_department_name()
        self.space_id = self.get_spaceid()

    def get_access_token(self):
        """获取钉钉访问令牌"""
        try:
            url = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
            payload = {
                "appKey": self.app_key,
                "appSecret": self.app_secret
            }
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            return response.json().get('accessToken')
        except Exception as e:
            logging.error(f"获取访问令牌失败: {str(e)}")
            return None

    def get_user_current_department_id(self):
        """获取用户当前部门ID"""
        try:
            dept_list, self.user_role = get_user_departments(self.access_token, self.dd_user_id)
            return dept_list[-1] if dept_list else 1
        except Exception as e:
            logging.error(f"获取用户部门ID失败: {str(e)}")
            return 1

    def get_user_current_department_name(self):
        """获取用户当前部门名称"""
        try:
            return get_department_name(self.access_token, self.dept_id)
        except Exception as e:
            logging.error(f"获取部门名称失败: {str(e)}")
            return "未知部门"

    def get_spaceid(self):
        """获取钉盘空间ID"""
        try:
            if not self.dd_user_id:
                return None

            url = "https://api.dingtalk.com/v1.0/workflow/processInstances/spaces/infos/query"
            payload = json.dumps({
                "userId": self.dd_user_id,
                "agentId": self.agent_id
            })
            headers = {
                'x-acs-dingtalk-access-token': self.access_token,
                'Content-Type': 'application/json'
            }

            response = requests.post(url, headers=headers, data=payload, timeout=30)
            result = response.json()
            if 'success' in result:
                return result['result']['spaceId']
            else:
                logging.error(f'获取spaceId失败：{result.get("errmsg", "未知错误")}')
                return None
        except Exception as e:
            logging.error(f'获取spaceId时发生错误：{str(e)}')
            return None

    def get_fileuploadinfo(self, space_id):
        """获取文件上传信息"""
        if not self.union_id:
            return None, None, None

        url = f"https://api.dingtalk.com/v1.0/storage/spaces/{self.space_id}/files/uploadInfos/query?unionId={self.union_id}"
        payload = json.dumps({
            "protocol": "HEADER_SIGNATURE",
            "multipart": False
        })
        headers = {
            'x-acs-dingtalk-access-token': self.access_token,
            'Content-Type': 'application/json'
        }

        try:
            response = requests.post(url, headers=headers, data=payload)
            result = response.json()
            if 'uploadKey' in result:
                resourceurl = result["headerSignatureInfo"]['resourceUrls'][0]
                headersreturned = result["headerSignatureInfo"]['headers']
                uploadkey = result['uploadKey']
                return uploadkey, resourceurl, headersreturned
            else:
                logging.error(f'获取文件上传信息失败：{result.get("errmsg", "未知错误")}')
                return None, None, None
        except Exception as e:
            logging.error(f'获取文件上传信息时发生错误：{str(e)}')
            return None, None, None

    def submitfieoss(self, resourceurls, resourceheaders, file_path):
        """上传文件到OSS"""
        try:
            result = requests.put(resourceurls, data=open(file_path, 'rb'), headers=resourceheaders)
            if result.status_code == 200:
                return 1
            return -1
        except Exception as e:
            logging.error(f'上传文件到oss时发生错误：{str(e)}')
            return -1

    def submitfie(self, space_id, uploadKey, file_path):
        """提交文件信息"""
        if not self.union_id:
            return None

        url = f"https://api.dingtalk.com/v1.0/storage/spaces/{self.space_id}/files/commit?unionId={self.union_id}"
        payload = json.dumps({
            "name": file_path,
            "uploadKey": uploadKey,
            "parentId": "0",
            "option": {
                "conflictStrategy": "AUTO_RENAME",
                "appProperties": [{"name": "testme", "visibility": "PUBLIC", "value": "testme"}]
            }
        })
        headers = {
            'x-acs-dingtalk-access-token': self.access_token,
            'Content-Type': 'application/json'
        }

        try:
            response = requests.post(url, headers=headers, data=payload)
            result = response.json()
            if 'dentry' in result:
                return result['dentry']
            else:
                logging.error(f'上传文件失败：{result.get("errmsg", "未知错误")}')
                return None
        except Exception as e:
            logging.error(f'上传文件时发生错误：{str(e)}')
            return None

    def create_approval(self, process_code, form_data, table_data, reason):
        """创建钉钉审批实例"""
        if not self.dd_user_id:
            st.error("用户信息不完整，无法创建审批")
            return {"error": "用户信息不完整"}

        url = "https://api.dingtalk.com/v1.0/workflow/processInstances"
        headers = {
            'x-acs-dingtalk-access-token': self.access_token,
            "Content-Type": "application/json"
        }

        dept_id = self.dept_id
        dept_name = self.dept_name

        # 计算冲销备用金金额和应付员工金额
        user_info = st.session_state.dingtalk_user
        user_id = user_info.get('userid', '')
        advance_balance_str = get_user_balance(user_id)
        advance_balance = 0.0
        if advance_balance_str:
            advance_balance = float(advance_balance_str.replace('¥', '').replace(',', ''))

        total_amount = form_data['total_amount_withtax']
        advance_amount = min(advance_balance, total_amount)
        payable_amount = total_amount - advance_amount

        # 根据业务活动类型确定项目字段
        activity_type = form_data['activity_type']
        project_field_id = ""
        if activity_type == "产品交付":
            project_field_id = "DDSelectField_W5QG9H22J3K0"  # 销售项目
        elif activity_type in ["研发费用化", "研发资本化"]:
            project_field_id = "DDSelectField_3VYIOELKEWA0"  # 研发项目

        form_values = [
            {
                "componentType": "DDSelectField",
                "name": "业务活动类型",
                "bizAlias": "",
                "id": "DDSelectField_101NDLQT0DBK0",
                "value": activity_type
            },
            {
                "componentType": "TextField",
                "name": "报销事由",
                "bizAlias": "",
                "id": "TextField_7UAZ9DS60DS0",
                "value": reason
            },
            {
                "componentType": "DDSelectField",
                "name": "费用类型",
                "bizAlias": "",
                "id": "DDSelectField_IGTRELP8IAW0",
                "value": form_data['expense_type']
            },
            {
                "componentType": "MoneyField",
                "name": "报销含税金额（元）",
                "bizAlias": "",
                "id": "MoneyField_Z5LH7RUAG1C0",
                "value": f"{form_data['total_amount_withtax']:.2f}"
            },
            {
                "componentType": "MoneyField",
                "name": "不含进项税金额（元）",
                "bizAlias": "",
                "id": "MoneyField_2KWCHXSYCM40",
                "value": f"{form_data['total_amount_withouttax']:.2f}"
            },
            {
                "componentType": "MoneyField",
                "name": "进项税额（元）",
                "bizAlias": "",
                "id": "MoneyField_1782GCAR7VWG0",
                "value": f"{form_data['total_amount_tax']:.2f}"
            },
            {
                "componentType": "NumberField",
                "name": "票据张数",
                "bizAlias": "",
                "id": "NumberField_7YM263WFLCK0",
                "value": str(form_data['ticket_count'])
            },
            {
                "componentType": "MoneyField",
                "name": "冲销备用金金额（元）",
                "bizAlias": "",
                "id": "MoneyField_IQ6443NK6UW0",
                "value": f"{advance_amount:.2f}"
            },
            {
                "componentType": "MoneyField",
                "name": "应付员工金额（元）",
                "bizAlias": "",
                "id": "MoneyField_6AWLA0JGFXO0",
                "value": f"{payable_amount:.2f}"
            },
            {
                "componentType": "TableField",
                "name": "报销明细",
                "bizAlias": "",
                "id": "TableField_12GBB3L3FC1C0",
                "value": table_data
            }
        ]

        # 添加项目字段（根据业务活动类型）
        if project_field_id and form_data['project_name']:
            if activity_type == "产品交付":
                form_values.append({
                    "componentType": "DDSelectField",
                    "name": "销售项目",
                    "bizAlias": "",
                    "id": project_field_id,
                    "value": form_data['project_name']
                })
            elif activity_type in ["研发费用化", "研发资本化"]:
                form_values.append({
                    "componentType": "DDSelectField",
                    "name": "研发项目",
                    "bizAlias": "",
                    "id": project_field_id,
                    "value": form_data['project_name']
                })

        payload = {
            "processCode": process_code,
            "originatorUserId": self.dd_user_id,
            "deptId": dept_id,
            "microappAgentId": self.agent_id,
            "originatorDeptName": dept_name,
            "formComponentValues": form_values
        }

        logging.info(f"创建审批请求数据: {json.dumps(payload, indent=2, ensure_ascii=False)}")

        try:
            response = requests.post(url, headers=headers, json=payload)
            response_data = response.json()
            return response_data
        except Exception as e:
            logging.error(f"审批创建请求异常: {str(e)}")
            return {"error": str(e)}

    def get_approval_instances(self, process_code):
        """获取审批实例"""
        try:
            now = datetime.now()
            start_time = int((now - timedelta(days=100)).timestamp() * 1000)

            url = "https://api.dingtalk.com/v1.0/workflow/processes/instanceIds/query"
            payload = json.dumps({
                "startTime": start_time,
                "processCode": process_code,
                "nextToken": 0,
                "maxResults": 20
            })
            headers = {
                'x-acs-dingtalk-access-token': self.access_token,
                'Content-Type': 'application/json'
            }

            response = requests.request("POST", url, headers=headers, data=payload)
            if response.status_code == 200:
                return response.json()['result']['list']
            else:
                logging.error(f"获取审批详情失败: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            logging.error(f"获取审批详情时出错: {str(e)}")
            return None

    def get_project_list(self, process_code):
        """获取项目列表"""
        instances = self.get_approval_instances(process_code)
        projectlist = []

        if instances is not None:
            for instance in instances:
                detail = self.get_approval_detail(instance)
                if detail and "formComponentValues" in detail:
                    formdetails = detail["formComponentValues"]
                    for item in formdetails:
                        if item.get("name") == "项目名称":
                            project_value = item.get("value", "")
                            if project_value and project_value not in projectlist:
                                projectlist.append(project_value)
        return projectlist if projectlist else None

    def get_approval_detail(self, instance_id):
        """获取单个审批实例的详细信息"""
        try:
            url = f"https://api.dingtalk.com/v1.0/workflow/processInstances?processInstanceId={instance_id}"
            headers = {
                'x-acs-dingtalk-access-token': self.access_token
            }

            response = requests.request("GET", url, headers=headers)
            if response.status_code == 200:
                return response.json()['result']
            else:
                logging.error(f"获取审批详情失败: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            logging.error(f"获取审批详情时出错: {str(e)}")
            return None


# 用户会话管理
class UserSessionManager:
    def __init__(self):
        self.sessions = {}

    def get_session(self, user_id):
        if user_id not in self.sessions:
            self.sessions[user_id] = {
                'uploaded_files': [],
                'ocr_results': [],
                'processed_files': {},
                'temp_files': {},
                'last_activity': time.time()
            }
        return self.sessions[user_id]

    def cleanup_expired_sessions(self, timeout=3600):  # 1小时超时
        current_time = time.time()
        expired_users = []
        for user_id, session in self.sessions.items():
            if current_time - session['last_activity'] > timeout:
                expired_users.append(user_id)

        for user_id in expired_users:
            self.cleanup_user_session(user_id)
            del self.sessions[user_id]

    def cleanup_user_session(self, user_id):
        if user_id in self.sessions:
            session = self.sessions[user_id]
            # 清理临时文件
            for file_path in session.get('temp_files', {}).values():
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except:
                    pass


# 初始化会话管理器
session_manager = UserSessionManager()


# 核心功能函数
def get_access_token(code):
    """使用授权码获取访问令牌"""
    try:
        timestamp = str(int(time.time() * 1000))
        signature = generate_signature(timestamp)

        if not signature:
            return None, None

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-acs-dingtalk-access-token": signature,
            "timestamp": timestamp
        }

        payload = {
            "clientId": DING_APP_KEY,
            "clientSecret": DING_APP_SECRET,
            "code": code,
            "grantType": "authorization_code"
        }

        response = requests.post("https://api.dingtalk.com/v1.0/oauth2/userAccessToken",
                                 headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        token_data = response.json()
        return token_data.get("accessToken"), token_data.get("expireIn")
    except Exception as e:
        logging.error(f"获取访问令牌失败: {str(e)}")
        return None, None


def generate_signature(timestamp):
    """生成钉钉API签名"""
    try:
        string_to_sign = f"{timestamp}\n{DING_APP_SECRET}"
        hmac_code = hmac.new(
            DING_APP_SECRET.encode('utf-8'),
            string_to_sign.encode('utf-8'),
            digestmod=hashlib.sha256
        ).digest()
        return base64.b64encode(hmac_code).decode('utf-8')
    except Exception as e:
        logging.error(f"生成签名失败: {str(e)}")
        return None


def get_user_info(access_token):
    """获取用户信息"""
    try:
        me_headers = {
            "x-acs-dingtalk-access-token": access_token,
            "Content-Type": "application/json"
        }
        me_response = requests.get(
            "https://api.dingtalk.com/v1.0/contact/users/me",
            headers=me_headers,
            timeout=30
        )
        me_response.raise_for_status()
        me_data = me_response.json()

        # 获取服务端access_token
        token_url = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
        token_payload = {
            "appKey": DING_APP_KEY,
            "appSecret": DING_APP_SECRET
        }
        token_response = requests.post(token_url, json=token_payload, timeout=30)
        token_data = token_response.json()
        corp_access_token = token_data.get("accessToken")

        if not corp_access_token:
            logging.error("获取服务端访问令牌失败")
            return None

        # 获取用户详细信息
        user_url = "https://oapi.dingtalk.com/topapi/user/getbyunionid"
        user_params = {
            "access_token": corp_access_token,
            "unionid": me_data.get("unionId")
        }
        user_response = requests.get(user_url, params=user_params, timeout=30)
        user_response.raise_for_status()
        user_data = user_response.json()

        if 'result' not in user_data:
            logging.error(f"用户信息API返回格式异常: {user_data}")
            return None

        user_info = user_data['result']

        # 获取用户部门和角色
        dept_list, user_role = get_user_departments(corp_access_token, user_info['userid'])
        if dept_list:
            deptid = dept_list[-1]
            deptname = get_department_name(corp_access_token, dept_list[-1])
            user_info['dept_name'] = deptname
            user_info['title'] = user_role

        # 合并基础信息和详细信息
        combined_data = {**me_data, **user_info}
        return combined_data
    except Exception as e:
        logging.error(f"获取用户信息失败: {str(e)}")
        return None


def get_dingtalk_auth_url():
    """生成钉钉授权URL"""
    try:
        params = {
            "response_type": "code",
            "client_id": DING_APP_KEY,
            "redirect_uri": DING_REDIRECT_URI,
            "scope": "openid corp",
            "state": "dingtalk_login",
            "prompt": "consent",
        }
        return f"https://login.dingtalk.com/oauth2/auth?{urllib.parse.urlencode(params)}"
    except Exception as e:
        logging.error(f"生成授权URL失败: {str(e)}")
        return None


def get_db_connection():
    """创建数据库连接"""
    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            connection = pymysql.connect(**DB_CONFIG)
            return connection
        except pymysql.MySQLError as e:
            error_code, error_msg = e.args
            logging.error(f"数据库连接失败 (尝试 {attempt + 1}/{max_retries}): {error_msg}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                return None
        except Exception as e:
            logging.error(f"未知错误: {str(e)}")
            return None


def get_user_balance(user_id):
    """查询指定员工的备用金余额"""
    connection = get_db_connection()
    if connection is None:
        return None

    try:
        with connection.cursor() as cursor:
            sql = "SELECT balance FROM advance_balance WHERE user_id = %s"
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
            return f"¥{result['balance']:,.2f}" if result else None
    except Exception as e:
        logging.error(f"查询用户余额失败: {str(e)}")
        return None
    finally:
        if connection:
            connection.close()


def get_user_departments(access_token, dd_user_id):
    """获取用户部门列表"""
    try:
        url = "https://oapi.dingtalk.com/topapi/v2/user/get"
        params = {"access_token": access_token}
        body = {"userid": dd_user_id}
        response = requests.post(url, params=params, json=body, timeout=30)
        response.raise_for_status()
        result = response.json()["result"]
        return result["dept_id_list"], result.get("title", "")
    except Exception as e:
        logging.error(f"获取用户部门失败: {str(e)}")
        return [], ""


def get_department_name(access_token, dept_id):
    """获取部门名称"""
    try:
        url = "https://oapi.dingtalk.com/topapi/v2/department/get"
        params = {"access_token": access_token}
        body = {"dept_id": dept_id}
        response = requests.post(url, params=params, json=body, timeout=30)
        response.raise_for_status()
        return response.json()["result"]["name"]
    except Exception as e:
        logging.error(f"获取部门名称失败: {str(e)}")
        return "未知部门"

def get_all_balances():
    """获取所有员工的备用金余额"""
    connection = get_db_connection()
    if connection is None:
        return None

    try:
        with connection.cursor() as cursor:
            sql = "SELECT user_id, balance, last_updated FROM advance_balance ORDER BY balance DESC"
            cursor.execute(sql)
            return cursor.fetchall()
    except Exception as e:
        logging.error(f"查询所有余额失败: {str(e)}")
        return None
    finally:
        if connection:
            connection.close()
# 备用金查询功能
def show_advance_fund_query():
    """显示备用金查询功能"""
    if not st.session_state.dingtalk_user:
        return

    user_info = st.session_state.dingtalk_user
    user_id = user_info.get('userid', '')

    roles = determine_user_roles(user_info)
    if "财务" not in roles and "总监" not in roles and "IT" not in roles:
        return

    with st.sidebar.expander("备用金查询"):
        st.subheader("备用金查询")

        option = st.radio("选择查询方式:",
                          ["查询指定员工余额", "查看所有员工余额"])

        if option == "查询指定员工余额":
            target_user_id = st.text_input("输入员工ID", value=user_id)

            if st.button("查询余额"):
                if target_user_id:
                    balance = get_user_balance(target_user_id)
                    if balance:
                        st.success(f"员工备用金余额: {balance}")
                    else:
                        st.warning("未找到该员工的备用金记录")
                else:
                    st.warning("请输入员工ID")

        elif option == "查看所有员工余额":
            if st.button("显示所有余额"):
                balances = get_all_balances()
                if balances:
                    df = pd.DataFrame(balances)
                    if 'last_updated' in df.columns:
                        df['last_updated'] = pd.to_datetime(df['last_updated']).dt.strftime('%Y-%m-%d %H:%M')
                    df.rename(columns={
                        'user_id': '员工ID',
                        'balance': '余额',
                        'last_updated': '更新时间'
                    }, inplace=True)

                    st.dataframe(df)

                    total_balance = df['余额'].sum()
                    avg_balance = df['余额'].mean()

                    st.markdown("""
                        <style>
                        div[data-testid="stMetricValue"] {
                            font-size: 20px !important;
                        }
                        div[data-testid="stMetricLabel"] {
                            font-size: 14px !important;
                        }
                        </style>
                    """, unsafe_allow_html=True)

                    col1, col2 = st.columns(2)
                    col1.metric("总备用金余额", f"¥{total_balance:,.2f}")
                    col2.metric("平均备用金余额", f"¥{avg_balance:,.2f}")
                else:
                    st.info("暂无备用金数据")


def check_user_permission(user_id):
    """检查用户权限"""
    return USER_ROLES.get(user_id, 'employee')


def get_allowed_expense_types(activity_type):
    """根据业务活动类型返回允许的费用类型列表和详细信息"""
    if not activity_type:
        return [], []

    connection = get_db_connection()
    if connection is None:
        return [], []

    try:
        with connection.cursor() as cursor:
            # 从数据库查询数据
            query = """
                    SELECT Code, Name, FullName, AccountingDimension, BalanceDirection, BusinessActivityType
                    FROM expense_type 
                    WHERE BusinessActivityType = %s
                """
            cursor.execute(query, (activity_type,))

            results = cursor.fetchall()
            if not results:
                return [], []

            # 初始化变量
            expense_names = []
            expense_details = []

            for row in results:
                # 构建费用详情字典
                expense_detail = {
                    "编码": row["Code"],
                    "名称": row["Name"],
                    "全名": row["FullName"],
                    "核算维度": row["AccountingDimension"],
                    "余额方向": row["BalanceDirection"],
                    "业务活动类型": row["BusinessActivityType"]
                }

                # 添加到匹配列表
                expense_details.append(expense_detail)

                # 添加费用名称到列表（去重）
                if row["Name"] not in expense_names:
                    expense_names.append(row["Name"])

            return expense_names, expense_details
    except Exception as e:
        logging.error(f"查询费用类型失败: {str(e)}")
        return [], []
    finally:
        if connection:
            connection.close()


def determine_user_roles(user_info):
    """根据用户信息确定用户角色"""
    roles = []
    dept_name = user_info.get('dept_name', '').lower()
    title = user_info.get('title', '').lower()

    # 根据部门和职位判断角色
    if '财务' in dept_name or '会计' in dept_name or '财务' in title:
        roles.append('财务')
    if '综合' in dept_name or 'IT' in dept_name or '技术' in dept_name:
        roles.append('IT')
    if '总监' in title or '经理' in title:
        roles.append('管理者')
    if '销售' in dept_name or '销售' in title:
        roles.append('销售')
    if '研发' in dept_name or '开发' in dept_name or '研发' in title:
        roles.append('研发')

    # 如果没有特定角色，设为普通员工
    if not roles:
        roles.append('普通员工')

    return roles

# 费用类型功能
def show_expense_types():
    """显示费用类型功能"""
    if not st.session_state.dingtalk_user:
        return

    user_info = st.session_state.dingtalk_user
    user_id = user_info.get('userid', '')

    roles = determine_user_roles(user_info)

    if "财务" not in roles and "IT" not in roles:
        return

    # 侧边栏信息
    with st.sidebar:
        st.subheader("费用类型")
        st.markdown("""
    - [查看更新费用类型](https://alidocs.dingtalk.com/i/nodes/pGBa2Lm8aG3a6ZMmc0NPomxMVgN7R35y)
    """)

def extract_archive(uploaded_file, extract_to):
    """解压上传的压缩文件到指定目录"""
    try:
        Path(extract_to).mkdir(parents=True, exist_ok=True)
        filename = uploaded_file.name.lower()

        if filename.endswith('.zip'):
            with zipfile.ZipFile(uploaded_file, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            return True, f"成功解压 ZIP 文件到 {extract_to}"
        elif filename.endswith(('.tar', '.tar.gz', '.tgz')):
            with tarfile.open(fileobj=uploaded_file, mode='r:*') as tar_ref:
                tar_ref.extractall(extract_to)
            return True, f"成功解压 TAR 文件到 {extract_to}"
        else:
            return False, "不支持的压缩格式"
    except Exception as e:
        return False, f"解压过程中出错: {str(e)}"


def classify_files(extract_dir):
    """根据文件名规则分类文件"""
    invoice_files = []
    support_files = []
    file_mapping = {}

    try:
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, extract_dir)

                # 根据文件名规则分类
                if re.match(r'^\d+_01_', file):
                    invoice_files.append({
                        'path': file_path,
                        'name': file,
                        'relative_path': relative_path
                    })
                    prefix = re.match(r'^(\d+)_', file).group(1)
                    if prefix not in file_mapping:
                        file_mapping[prefix] = {'invoice': [], 'support': []}
                    file_mapping[prefix]['invoice'].append(file_path)
                elif re.match(r'^\d+_02_', file):
                    support_files.append({
                        'path': file_path,
                        'name': file,
                        'relative_path': relative_path
                    })
                    prefix = re.match(r'^(\d+)_', file).group(1)
                    if prefix not in file_mapping:
                        file_mapping[prefix] = {'invoice': [], 'support': []}
                    file_mapping[prefix]['support'].append(file_path)

        invoice_files.sort(key=lambda x: x['name'])
        support_files.sort(key=lambda x: x['name'])
        return invoice_files, support_files, file_mapping
    except Exception as e:
        logging.error(f"文件分类失败: {str(e)}")
        return [], [], {}


def process_uploaded_archive(uploaded_file):
    """处理上传的压缩文件"""
    try:
        temp_dir = tempfile.mkdtemp(prefix=f"user_{st.session_state.get('user_session_id', 'default')}_")
        extract_dir = os.path.join(temp_dir, 'extracted')

        success, message = extract_archive(uploaded_file, extract_dir)
        if not success:
            return False, message, [], [], {}

        invoice_files, support_files, file_mapping = classify_files(extract_dir)

        # 保存到session state
        st.session_state.extracted_files = {
            'temp_dir': temp_dir,
            'extract_dir': extract_dir
        }

        return True, "文件解压和分类成功", invoice_files, support_files, file_mapping
    except Exception as e:
        return False, f"处理压缩文件失败: {str(e)}", [], [], {}


def ocr_invoice(filelist):
    """调用智谱AI票据识别服务"""
    try:
        api_key = invoice_recognizer.API_KEY
        api_url = invoice_recognizer.API_URL
        recognizer = InvoiceRecognizer(api_key, api_url)

        all_results = []
        for file_path in filelist:
            if not os.path.exists(file_path):
                logging.warning(f"文件不存在: {file_path}")
                continue

            logging.info(f"正在处理文件: {file_path}")
            result = recognizer.recognize_file(file_path)

            if "error" in result:
                logging.error(f"处理失败: {result['error']}")
            else:
                all_results.append(result)
                logging.info(f"处理成功: {json.dumps(result, ensure_ascii=False)}")

        return all_results

    except Exception as e:
        st.error(f"票据识别出错: {str(e)}")
        return []


def build_combined_table_data(invoice_results, file_mapping):
    """构建包含发票和支持文件的表格数据"""
    table_data = []

    # 处理每个发票文件
    for result_index, result in enumerate(invoice_results):
        # 获取对应的文件名
        if result_index < len(st.session_state.invoice_files):
            file_info = st.session_state.invoice_files[result_index]
            file_name = file_info['name']
            file_path = file_info['path']
        else:
            continue

        amount_value = result.get('total_amount', 0.0)
        tickets = result.get('tickets', [])

        if not isinstance(tickets, list):
            tickets = [tickets] if tickets else []

        # 处理每个票据
        for ticket in tickets:
            ticket_type = ticket.get('票据类型', '')

            # 标准化票据类型
            if "火车" in ticket_type or "车票" in ticket_type:
                ticket_type = "火车票"
            elif "飞机" in ticket_type or "机票" in ticket_type:
                ticket_type = "机票"
            elif "出租" in ticket_type or "租车" in ticket_type:
                ticket_type = "出租车票"
            elif "住宿" in ticket_type or "酒店" in ticket_type:
                ticket_type = "住宿费"
            elif "餐饮" in ticket_type or "饭店" in ticket_type:
                ticket_type = "餐饮"
            elif "运输" in ticket_type or "物流" in ticket_type:
                ticket_type = "运输服务"
            elif "快递" in ticket_type:
                ticket_type = "快递服务"
            elif "维修" in ticket_type:
                ticket_type = "维修服务"
            elif "咨询" in ticket_type:
                ticket_type = "咨询服务"
            elif "设计" in ticket_type:
                ticket_type = "设计服务"

            try:
                amount_value = float(
                    str(amount_value).replace('￥', '').replace(',', '').replace('元', ''))
            except:
                amount_value = 0.0

            tax_amount = result.get('tax_amount', 0.0)
            amount_excluding_tax = amount_value - tax_amount

            # 获取进项税类型
            tax_type = result.get('tax_type', '增值税专用发票')

            project_display = st.session_state.global_project_name if st.session_state.global_project_name else "部门"

            # 添加发票文件行
            table_data.append({
                "文件": file_name,
                "票据类型": ticket_type,
                "开票日期": ticket.get('开票日期', ''),
                "报销含税金额": amount_value,
                "进项税额": tax_amount,
                "不含进项税金额": amount_excluding_tax,
                "进项税类型": tax_type,
                "业务活动类型": st.session_state.global_activity_type,
                "项目名称": project_display,
                "费用类型": st.session_state.selected_expense_type
            })

    return table_data

def build_table_data(file_dicts, df):
    """构建钉钉动态表格所需的数据结构"""
    result_list = []
    for idx, row in df.iterrows():
        file_name = row.get("文件", "")
        # 查找匹配的文件信息
        matched_files = [f for f in file_dicts if f["originalFileName"] == file_name]

        for file_info in matched_files:
            # 查找该票据对应的支持文件
            support_files = []
            prefix = re.match(r'^(\d+)_', file_name)
            if prefix:
                prefix_str = prefix.group(1)
                if prefix_str in st.session_state.file_mapping:
                    for support_file_path in st.session_state.file_mapping[prefix_str]['support']:
                        support_file_name = os.path.basename(support_file_path)
                        support_file_info = next((f for f in file_dicts if f["originalFileName"] == support_file_name),
                                                 None)
                        if support_file_info:
                            support_files.append(support_file_info)

            # 合并发票文件和支持文件
            all_attachments = [file_info] + support_files

            row_data = [
                {
                    "componentName": "DDAttachment",
                    "name": "附件",
                    "value": [{
                        "spaceId": str(attachment["spaceId"]),
                        "fileName": attachment["fileName"],
                        "fileSize": int(attachment["fileSize"]),
                        "fileType": attachment["fileType"],
                        "fileId": attachment["fileId"]
                    } for attachment in all_attachments]
                },
                {
                    "componentName": "TextField",
                    "name": "票据类型",
                    "value": row["票据类型"]
                },
                {
                    "componentName": "DDDateField",
                    "name": "日期",
                    "value": convert_date_format(row["开票日期"])
                },
                {
                    "componentName": "MoneyField",
                    "name": "报销含税金额（元）",
                    "value": f"{row['报销含税金额']:.2f}"
                },
                {
                    "componentName": "MoneyField",
                    "name": "不含进项税金额（元）",
                    "value": f"{row['不含进项税金额']:.2f}"
                },
                {
                    "componentName": "MoneyField",
                    "name": "进项税额（元）",
                    "value": f"{row['进项税额']:.2f}"
                },
                {
                    "componentName": "DDSelectField",
                    "name": "进项税类型",
                    "value": row.get("进项税类型", "无")
                },
                {
                    "componentName": "DDSelectField",
                    "name": "项目/部门",
                    "value": row["项目名称"]
                }
            ]
            result_list.append(row_data)
    return result_list


def convert_date_format(date_str):
    """转换日期格式"""
    try:
        if not date_str or pd.isna(date_str):
            return ""

        # 尝试多种日期格式
        date_formats = [
            "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d",
            "%Y年%m月%d日", "%Y-%m", "%Y/%m"
        ]

        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(str(date_str), fmt)
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # 如果都不匹配，返回原始字符串
        return str(date_str)
    except Exception as e:
        logging.error(f"日期格式转换失败: {str(e)}")
        return str(date_str)


# 核心应用类
class SmartInvoiceApp:
    def __init__(self):
        self.static_content = load_static_content()
        self.user_session = None

    def init_session_state(self):
        """初始化会话状态"""
        default_states = {
            'dingtalk_user': None,
            'access_token': None,
            'global_activity_type': None,
            'global_project_name': None,
            'selected_expense_type': None,
            'uploader_key': 0,
            'ocr_processed': False,
            'approval_submitted': False,
            'user_session_id': str(uuid.uuid4())[:8],
            'pending_refresh': False,
            'invoice_files': [],
            'support_files': [],
            'file_mapping': {},
            'extracted_files': {},
            'editable_df': None,
            'revision_confirmed': False,
            'files_uploaded': False,
            'all_ocr_results': [],
            'approval_instance_id': None,
            'business_info_confirmed': False,
            'user_roles': []
        }

        for key, value in default_states.items():
            if key not in st.session_state:
                st.session_state[key] = value

    def render_static_content(self):
        """渲染静态HTML内容"""
        st.markdown(self.static_content, unsafe_allow_html=True)

    def handle_authentication(self):
        """处理用户认证"""
        # 总是显示应用名称
        st.markdown('<h1 class="main-header">智能票据审批系统</h1>', unsafe_allow_html=True)

        if st.session_state.dingtalk_user:
            return True

        code = st.query_params.get("code")
        if code:
            with st.spinner("🔒 正在验证登录信息..."):
                access_token, expire_in = get_access_token(code)
                if access_token:
                    st.session_state.access_token = access_token
                    user_info = get_user_info(access_token)
                    if user_info:
                        st.session_state.dingtalk_user = user_info
                        # 确定用户角色
                        st.session_state.user_roles = determine_user_roles(user_info)
                        user_id = user_info.get('userid', 'default')
                        self.user_session = session_manager.get_session(user_id)
                        params = dict(st.query_params)
                        if "code" in params:
                            del params["code"]
                            st.query_params.clear()
                            st.query_params.update(params)
                        st.rerun()
            return False
        else:
            auth_url = get_dingtalk_auth_url()
            if auth_url:
                st.markdown("""
                <div style="text-align: center; padding: 2rem;">
                    <h3>钉钉免登</h3>
                    <p>请使用钉钉账号登录以继续</p>
                    <a href="{}" target="_blank" style="display: inline-block; padding: 0.8rem 1.5rem; background-color: #0086FA; color: white; border-radius: 8px; font-weight: 600; text-decoration: none;">
                        🔒 钉钉账号登录
                    </a>
                </div>
                """.format(auth_url), unsafe_allow_html=True)
            return False

    def render_user_info(self):
        """渲染用户信息"""
        if not st.session_state.dingtalk_user:
            return

        user_info = st.session_state.dingtalk_user
        with st.sidebar:
            with st.container():
                st.subheader("👤 用户信息")
                cols = st.columns([1, 3])
                with cols[0]:
                    if user_info.get("avatarUrl"):
                        st.image(user_info["avatarUrl"], width=100)
                    else:
                        st.image("https://via.placeholder.com/100", width=100)

                with cols[1]:
                    st.markdown(f"**姓名**: {user_info.get('nick', '')}")
                    st.markdown(f"**职位**: {user_info.get('title', '')}")
                    roles = determine_user_roles(user_info)
                    st.markdown(f"**角色**: {', '.join(roles)}")
                    st.markdown(f"**部门**: `{user_info.get('dept_name', '')}`")

            balance = get_user_balance(user_info.get('userid', ''))
            if balance:
                st.write(f"备用金余额: {balance}")

            if st.button("退出登录", use_container_width=False):
                st.session_state.dingtalk_user = None
                st.session_state.access_token = None
                if self.user_session:
                    session_manager.cleanup_user_session(user_id)
                st.rerun()

    def render_selection_form(self):
        """渲染业务选择表单"""
        st.markdown('<div class="custom-subheader">步骤1: 选择业务信息</div>', unsafe_allow_html=True)

        with st.container():
            # st.markdown('<div class="step-container">', unsafe_allow_html=True)

            # 从数据库获取业务活动类型
            activity_types = ["产品交付", "生产制造", "研发费用化", "研发资本化", "销售费用", "管理费用"]

            activity_type = st.selectbox(
                "1. 请选择本次报销的业务活动类型 *",
                options=activity_types,
                index=0,
                key="activity_type_select"
            )

            expense_type = None
            if activity_type:
                # 从数据库获取允许的费用类型
                allowed_expense_types, expense_details = get_allowed_expense_types(activity_type)
                if allowed_expense_types:
                    expense_type = st.selectbox(
                        "2. 请选择费用类型 *",
                        options=allowed_expense_types,
                        key="expense_type_select"
                    )
                    # 存储费用详情
                    if expense_type and expense_details:
                        for detail in expense_details:
                            if detail["名称"] == expense_type:
                                st.session_state.expense_full_name = detail["全名"]
                                break
                else:
                    st.error(f"当前业务活动类型 '{activity_type}' 没有配置费用类型，请联系管理员")

            project_name = ""
            ding = DingTalkApproval()
            if activity_type == "产品交付":
                sales_projects = ding.get_project_list(DING_PROCESS_CODE_MARKET)
                if sales_projects is not None:
                    project_name = st.selectbox(
                        "请选择产品交付关联的销售项目 *",
                        options=sales_projects,
                        key="sales_project_select"
                    )
                else:
                    st.error("无法获取销售项目列表，请稍后重试")
            elif activity_type in ["研发费用化", "研发资本化"]:
                rd_projects = ding.get_project_list(DING_PROCESS_CODE_RD)
                if rd_projects is not None:
                    project_name = st.selectbox(
                        "请选择研发活动关联的研发项目 *",
                        options=rd_projects,
                        key="rd_project_select"
                    )
                else:
                    st.error("无法获取研发项目列表，请稍后重试")

            # 自动确认业务信息，不需要按钮
            if activity_type and expense_type:
                if activity_type in ["产品交付", "研发费用化", "研发资本化"]:
                    if project_name:
                        st.session_state.global_activity_type = activity_type
                        st.session_state.global_project_name = project_name
                        st.session_state.selected_expense_type = expense_type
                        st.session_state.business_info_confirmed = True
                else:
                    st.session_state.global_activity_type = activity_type
                    st.session_state.global_project_name = project_name
                    st.session_state.selected_expense_type = expense_type
                    st.session_state.business_info_confirmed = True

            st.markdown('</div>', unsafe_allow_html=True)

            return activity_type, expense_type, project_name

    def render_file_upload_and_preview(self):
        """渲染文件上传和预览界面（合并为一个步骤）"""
        st.markdown('<div class="custom-subheader">步骤2: 上传和预览文件</div>', unsafe_allow_html=True)

        with st.container():
            # st.markdown('<div class="step-container">', unsafe_allow_html=True)

            st.markdown("""
            <div class="info-box">
                💡 请上传包含所有票据文件的压缩文件<br>
                • 文件命名规则：发票文件为0N_01_xxx，支持文件为0N_02_xxx（N为数字）<br>
                • 支持格式: ZIP, TAR, TAR.GZ<br>
                • 最大文件大小: 100MB
            </div>
            """, unsafe_allow_html=True)

            uploaded_file = st.file_uploader(
                "选择压缩文件",
                type=['zip', 'tar', 'tar.gz', 'tgz'],
                key=f'file_uploader_{st.session_state.uploader_key}'
            )

            # 处理上传的文件并显示预览
            if uploaded_file:
                success, message, invoice_files, support_files, file_mapping = process_uploaded_archive(uploaded_file)

                if success:
                    # 存储文件信息到session state
                    st.session_state.invoice_files = invoice_files
                    st.session_state.support_files = support_files
                    st.session_state.file_mapping = file_mapping
                    st.session_state.files_uploaded = True

                    # 显示文件预览
                    st.markdown("##### 文件预览")
                    self.render_file_preview(invoice_files, support_files, file_mapping)

                    return True
                else:
                    st.error(f"文件处理失败: {message}")
                    return False

            st.markdown('</div>', unsafe_allow_html=True)

            return False

    def render_file_preview(self, invoice_files, support_files, file_mapping):
        """渲染文件预览界面"""
        if not invoice_files and not support_files:
            return

        # 创建文件组
        file_groups = self.create_file_groups(invoice_files, support_files, file_mapping)

        # 显示所有文件组
        for group in file_groups:
            st.write(f"**报销明细 {group['prefix']}:**")

            # 显示发票文件
            for invoice_file in group['invoice_files']:
                self.display_file_preview_combined(invoice_file['name'], invoice_file['path'])

            # 显示支持文件
            for support_file in group['support_files']:
                self.display_file_preview_combined(support_file['name'], support_file['path'])

    def create_file_groups(self, invoice_files, support_files, file_mapping):
        """创建文件组，将发票文件和支持文件按组分类"""
        file_groups = []

        # 按前缀分组
        for prefix, files in file_mapping.items():
            group = {
                'prefix': prefix,
                'invoice_files': [],
                'support_files': []
            }

            # 添加发票文件
            for invoice_path in files['invoice']:
                invoice_file = next((f for f in invoice_files if f['path'] == invoice_path), None)
                if invoice_file:
                    group['invoice_files'].append(invoice_file)

            # 添加支持文件
            for support_path in files['support']:
                support_file = next((f for f in support_files if f['path'] == support_path), None)
                if support_file:
                    group['support_files'].append(support_file)

            file_groups.append(group)

        # 按前缀排序
        file_groups.sort(key=lambda x: x['prefix'])

        return file_groups

    def display_file_preview_combined(self, filename, file_path):
        """合并显示文件信息和预览 - 移除删除按钮"""
        try:
            file_ext = os.path.splitext(filename)[1].lower()

            # 创建可折叠区域，标题包含文件信息
            with st.expander(f"📄 {filename}", expanded=False):
                # 只显示文件预览，移除删除按钮
                if file_ext == '.pdf':
                    self.display_pdf(file_path)
                elif file_ext in ['.jpg', '.jpeg', '.png', '.bmp', '.gif']:
                    self.display_image(file_path)
                else:
                    st.warning(f"不支持预览的文件类型: {file_ext}")
        except Exception as e:
            st.error(f"预览文件失败: {str(e)}")

    def display_pdf(self, file_path):
        """使用 PyMuPDF 渲染 PDF 为高质量图像"""
        try:
            doc = fitz.open(file_path)
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                img_data = pix.tobytes("png")
                st.image(
                    img_data,
                    caption=f"{os.path.basename(file_path)} - 第 {page_num + 1} 页",
                    width='stretch'
                )
            doc.close()
        except Exception as e:
            st.error(f"PDF渲染失败: {str(e)}")

    def display_image(self, file_path):
        """显示图片文件"""
        try:
            st.image(
                file_path,
                caption=os.path.basename(file_path),
                width='stretch'
            )
        except Exception as e:
            st.error(f"图片显示失败: {str(e)}")

    def render_ocr_processing(self):
        """渲染OCR处理界面"""
        st.markdown('<div class="custom-subheader">步骤3: 识别票据</div>', unsafe_allow_html=True)

        with st.container():
            # st.markdown('<div class="step-container">', unsafe_allow_html=True)

            if not st.session_state.ocr_processed:
                st.info("请点击下方按钮开始识别票据内容")

                if st.button("开始识别票据", type="primary", icon='👓', use_container_width=True):
                    with st.spinner("正在识别票据，请稍候..."):
                        invoice_files = st.session_state.invoice_files
                        invoice_file_paths = [file_info['path'] for file_info in invoice_files]
                        ocr_results = ocr_invoice(invoice_file_paths)

                        if ocr_results:
                            st.session_state.all_ocr_results = ocr_results
                            st.session_state.ocr_processed = True

                            # 构建表格数据
                            table_data = build_combined_table_data(ocr_results, st.session_state.file_mapping)
                            if table_data:
                                df = pd.DataFrame(table_data)
                                st.session_state.editable_df = df
                                st.success("票据识别完成！")
                                st.rerun()
                        else:
                            st.error("票据识别失败，请重试")
            else:
                st.success("✅ 票据识别已完成")

            st.markdown('</div>', unsafe_allow_html=True)

    def render_invoice_table(self):
        """渲染票据表格"""
        st.markdown('<div class="custom-subheader">步骤4: 确认票据信息</div>', unsafe_allow_html=True)

        with st.container():
            # st.markdown('<div class="step-container">', unsafe_allow_html=True)

            if st.session_state.ocr_processed and st.session_state.editable_df is not None:
                df = st.session_state.editable_df

                # 显示汇总信息
                st.markdown("##### 票据汇总")

                total_with_tax = df["报销含税金额"].sum()
                total_without_tax = df["不含进项税金额"].sum()
                total_tax = df["进项税额"].sum()

                st.markdown("""
                    <style>
                    div[data-testid="stMetricValue"] {
                        font-size: 20px !important;
                    }
                    div[data-testid="stMetricLabel"] {
                        font-size: 14px !important;
                    }
                    </style>
                """, unsafe_allow_html=True)
                col1, col2, col3 = st.columns(3)
                col1.metric("报销含税金额", f"¥{total_with_tax:.2f}")
                col2.metric("不含进项税金额", f"¥{total_without_tax:.2f}")
                col3.metric("进项税额", f"¥{total_tax:.2f}")

                # 显示可编辑表格
                st.markdown("##### 票据详情")

                with st.form("invoice_editable_table_form"):
                    edited_df = st.data_editor(
                        df,
                        column_config={
                            "票据类型": st.column_config.TextColumn(
                                "票据类型",
                                width="medium",
                                disabled=True
                            ),
                            "开票日期": st.column_config.TextColumn(
                                "开票日期",
                                width="small",
                                disabled=True
                            ),
                            "报销含税金额": st.column_config.NumberColumn(
                                "报销含税金额 (¥)",
                                format="¥%.2f",
                                min_value=0.0,
                                step=0.01,
                                disabled=False,
                                width="small"
                            ),
                            "不含进项税金额": st.column_config.NumberColumn(
                                "不含税金额 (¥)",
                                format="¥%.2f",
                                min_value=0.0,
                                step=0.01,
                                disabled=False,
                                width="small"
                            ),
                            "进项税额": st.column_config.NumberColumn(
                                "进项税额 (¥)",
                                format="¥%.2f",
                                min_value=0.0,
                                step=0.01,
                                disabled=False,
                                width="small"
                            ),
                            "进项税类型": st.column_config.SelectboxColumn(
                                "进项税类型",
                                options=["勾选抵扣", "计算抵扣运输服务", "无"],
                                required=True,
                                width="medium"
                            ),
                            "业务活动类型": st.column_config.TextColumn(
                                "业务活动类型",
                                disabled=True,
                                width="medium"
                            ),
                            "项目名称": st.column_config.TextColumn(
                                "项目/部门",
                                disabled=True,
                                width="medium"
                            ),
                            "费用类型": st.column_config.TextColumn(
                                "费用类型",
                                disabled=True,
                                width="medium"
                            )
                        },
                        hide_index=True,
                        use_container_width=True,
                        key="invoice_editor"
                    )

                    if st.form_submit_button("确认票据信息", type="primary", icon='✔',use_container_width=True):
                        st.session_state.editable_df = edited_df
                        st.session_state.revision_confirmed = True
                        st.success("票据信息已确认!")
                        st.rerun()

            st.markdown('</div>', unsafe_allow_html=True)

    def submit_approval_to_dingtalk(self, total_amount, reason, df):
        """提交审批到钉钉系统"""
        try:
            ding = DingTalkApproval()

            # 上传文件到钉盘
            all_file_details = []
            process_code = DING_PROCESS_CODE

            # 上传所有文件（发票文件+支持文件）
            all_files = st.session_state.invoice_files + st.session_state.support_files

            for file_info in all_files:
                file_path = file_info['path']
                file_name = file_info['name']

                # 获取钉盘空间ID
                spaceId = ding.space_id
                if not spaceId:
                    st.error(f"获取钉盘空间失败: {file_name}")
                    continue

                # 上传文件到钉盘
                with st.spinner(f"上传附件到钉钉审批空间({file_name})..."):
                    uploadKey, resourceurl, resourceheaders = ding.get_fileuploadinfo(spaceId)
                    if not uploadKey:
                        st.error(f"获取文件上传信息失败: {file_name}")
                        continue

                    # 上传文件到OSS
                    submittedoss = ding.submitfieoss(resourceurl, resourceheaders, file_path)
                    if submittedoss == -1:
                        st.error(f"上传文件到OSS失败: {file_name}")
                        continue

                    # 提交文件信息
                    file_info_result = ding.submitfie(spaceId, uploadKey, file_name)
                    if not file_info_result:
                        st.error(f"上传文件失败: {file_name}")
                        continue

                    filedetails = {
                        'fileSize': file_info_result['size'],
                        'spaceId': spaceId,
                        'fileName': file_info_result['name'],
                        'fileType': file_info_result['extension'],
                        'originalFileName': file_name,
                        'fileId': file_info_result['id']
                    }
                    all_file_details.append(filedetails)

            # 准备审批数据
            form_data = {
                "total_amount_withtax": float(total_amount),
                "total_amount_withouttax": float(df["不含进项税金额"].sum()),
                "total_amount_tax": float(df["进项税额"].sum()),
                "activity_type": st.session_state.global_activity_type,
                "project_name": st.session_state.global_project_name if st.session_state.global_project_name else "部门",
                "expense_type": st.session_state.selected_expense_type,
                "ticket_count": len(df)
            }

            # 构建动态表格数据
            table_data_list = build_table_data(all_file_details, df)
            table_data_str = json.dumps(table_data_list, ensure_ascii=False)

            # 提交审批
            with st.spinner("正在创建审批流程..."):
                response = ding.create_approval(
                    process_code,
                    form_data,
                    table_data_str,
                    reason
                )

            if 'instanceId' in response:
                return True, f"审批单创建成功! 审批编号: {response['instanceId']}", response['instanceId']
            else:
                error_msg = response.get('message', '未知错误')
                if 'errmsg' in response:
                    error_msg = response['errmsg']
                elif 'error_description' in response:
                    error_msg = response['error_description']
                elif 'error' in response:
                    error_msg = response['error']

                return False, f"提交失败: {error_msg}", None

        except Exception as e:
            logging.exception("提交审批异常")
            return False, f"提交审批过程中发生错误: {str(e)}", None

    def render_approval_submission(self):
        """渲染审批提交界面"""
        st.markdown('<div class="custom-subheader">步骤5: 提交审批</div>', unsafe_allow_html=True)

        with st.container():
            st.markdown('<div class="step-container">', unsafe_allow_html=True)

            if st.session_state.get('revision_confirmed', False):
                df = st.session_state.editable_df
                total_amount = df["报销含税金额"].sum()

                st.markdown(f"##### 总报销金额: ¥{total_amount:.2f}")

                # 查询备用金余额
                user_info = st.session_state.dingtalk_user
                user_id = user_info.get('userid', '')
                advance_balance_str = get_user_balance(user_id)

                if advance_balance_str:
                    advance_balance = float(advance_balance_str.replace('¥', '').replace(',', ''))

                    if advance_balance >= total_amount:
                        st.markdown(
                            f"**有备用金：¥{advance_balance:.2f}元。审批通过后还有备用金:¥{advance_balance - total_amount:.2f}元**。")
                    elif advance_balance > 0:
                        st.markdown(
                            f"**有备用金：¥{advance_balance:.2f}元。审批通过后报销：¥{total_amount - advance_balance:.2f}元，再没有备用金。**")
                    else:
                        st.markdown(
                            f"**没有备用金。审批通过后报销：¥{total_amount:.2f}元。**")

                with st.form("approval_form"):
                    reason = st.text_input(
                        "报销事由",
                        value=st.session_state.get('expense_full_name', '日常费用报销')
                    )

                    agree = st.checkbox("我确认所有票据信息正确无误", value=False)

                    submitted = st.form_submit_button("提交审批", icon="🧨", use_container_width=True)

                    if submitted:
                        if not agree:
                            st.error("请确认票据信息正确")
                        elif not reason:
                            st.error("请输入报销事由")
                        else:
                            # 实际提交审批到钉钉
                            success, message, instance_id = self.submit_approval_to_dingtalk(total_amount, reason, df)

                            if success:
                                st.success(message)
                                st.balloons()
                                st.session_state.approval_submitted = True
                                st.session_state.approval_instance_id = instance_id

                                # 清理临时文件
                                if st.session_state.extracted_files:
                                    temp_dir = st.session_state.extracted_files.get('temp_dir')
                                    if temp_dir and os.path.exists(temp_dir):
                                        try:
                                            shutil.rmtree(temp_dir)
                                        except Exception as e:
                                            logging.error(f"清理临时目录失败: {str(e)}")

                                st.session_state.uploader_key += 1
                                st.rerun()
                            else:
                                st.error(message)

            st.markdown('</div>', unsafe_allow_html=True)

    def run(self):
        """运行主应用"""
        # 初始化
        self.init_session_state()
        self.render_static_content()

        # 清理过期会话
        session_manager.cleanup_expired_sessions()

        # 用户认证
        if not self.handle_authentication():
            return

        # 显示用户信息
        self.render_user_info()
        show_advance_fund_query()
        show_expense_types()
        # 如果审批已提交，显示成功页面
        if st.session_state.approval_submitted:
            # 安全地获取审批实例ID
            approval_instance_id = getattr(st.session_state, 'approval_instance_id', None)

            st.markdown(f"""
            <div style="text-align: center; padding: 4rem;"> 
                <h2 style="color: green;">✅ 审批提交成功！</h2>
                <p style="font-size: 1.2rem;">您的报销审批已成功提交到钉钉系统</p>
                <p><strong>审批编号: {approval_instance_id or "未知"}</strong></p>
                <p>您可以在钉钉应用中查看审批进度</p>
                <br>
            </div>
            """, unsafe_allow_html=True)

            if st.button("提交新的报销", icon='👈',use_container_width=True):
                # 重置状态
                for key in list(st.session_state.keys()):
                    if key not in ['dingtalk_user', 'access_token', 'user_session_id', 'user_roles']:
                        del st.session_state[key]
                st.rerun()
            return

        # 步骤1: 业务选择
        activity_type, expense_type, project_name = self.render_selection_form()

        # 只有当业务信息确认后才显示后续步骤
        if st.session_state.business_info_confirmed:
            # 步骤2: 文件上传和预览（合并为一个步骤）
            files_processed = self.render_file_upload_and_preview()

            if files_processed:
                # 步骤3: OCR处理
                self.render_ocr_processing()

                # 步骤4: 显示票据表格（如果已处理）
                if st.session_state.ocr_processed:
                    self.render_invoice_table()

                    # 步骤5: 审批提交
                    if st.session_state.get('revision_confirmed', False):
                        self.render_approval_submission()


# 运行应用
if __name__ == "__main__":
    app = SmartInvoiceApp()
    app.run()