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

# 钉钉应用配置
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

# 自定义CSS
st.markdown("""
    <style>
        .main-header {
            font-size: 2.5rem;
            color: #1f77b4;
            text-align: center;
            margin-bottom: 2rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid #1f77b4;
        }
        .user-info {
            position: absolute;
            top: 10px;
            right: 10px;
            background-color: #f0f2f6;
            padding: 10px;
            border-radius: 5px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            z-index: 999;
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
        .info-box {
            background-color: #d1ecf1;
            color: #0c5460;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            border: 1px solid #bee5eb;
        }
        .stDataFrame {
            width: 100% !important;
        }
        .stDataFrame div[data-testid="stHorizontalBlock"] {
            width: 100% !important;
        }
        .stDataFrame table {
            width: 100% !important;
        }
        .stDataFrame th, .stDataFrame td {
            min-width: 120px;
            max-width: 200px;
            white-space: normal !important;
        }
        .file-preview {
            border: 1px solid #ddd;
            border-radius: 5px;
            padding: 10px;
            margin-bottom: 10px;
        }
        .file-actions {
            display: flex;
            justify-content: space-between;
            margin-top: 10px;
        }
    </style>
""", unsafe_allow_html=True)


# 在文件顶部定义函数
def custom_subheader(text, font_size=24, color="#1f77b4"):
    st.markdown(
        f'<h2 style="font-size: {font_size}px; color: {color}; font-weight: bold; margin-bottom: 20px;">{text}</h2>',
        unsafe_allow_html=True
    )


def custom_warning(message):
    st.markdown(f"""
    <div style="
        background-color: #fff3cd; 
        border: 1px solid #ffeaa7; 
        color: #FF0000; 
        padding: 10px; 
        border-radius: 4px; 
        font-size: 20px; 
        font-family: 'Arial', sans-serif;
        margin-bottom: 1rem;">
        ⚠️ {message}
    </div>
    """, unsafe_allow_html=True)


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# 初始化session状态
def init_session_state():
    if 'dingtalk_user' not in st.session_state:
        st.session_state.dingtalk_user = None
    if 'dingtalk_dept' not in st.session_state:
        st.session_state.dingtalk_dept = None
    if 'access_token' not in st.session_state:
        st.session_state.access_token = None
    if 'global_activity_type' not in st.session_state:
        st.session_state.global_activity_type = None
    if 'global_project_name' not in st.session_state:
        st.session_state.global_project_name = None
    if 'uploader_key' not in st.session_state:
        st.session_state.uploader_key = 0
    if 'uploaded_files' not in st.session_state:
        st.session_state.uploaded_files = []
    if 'file_previews' not in st.session_state:
        st.session_state.file_previews = {}
    if 'all_ocr_results' not in st.session_state:
        st.session_state.all_ocr_results = []
    if 'processed_files' not in st.session_state:
        st.session_state.processed_files = {}
    if 'selected_invoice' not in st.session_state:
        st.session_state.selected_invoice = 0
    if 'activity_valid' not in st.session_state:
        st.session_state.activity_valid = False
    if 'file_previews' not in st.session_state:
        st.session_state.file_previews = {}
    if 'ocr_processed' not in st.session_state:
        st.session_state.ocr_processed = False
    if 'editable_df' not in st.session_state:
        st.session_state.editable_df = None
    if 'temp_files' not in st.session_state:
        st.session_state.temp_files = {}
    if 'selected_approvals' not in st.session_state:
        st.session_state.selected_approvals = []
    if 'selected_expense_type' not in st.session_state:
        st.session_state.selected_expense_type = None
    if 'approval_submitted' not in st.session_state:
        st.session_state.approval_submitted = False
    if 'approval_instance_id' not in st.session_state:
        st.session_state.approval_instance_id = None
    if 'extracted_files' not in st.session_state:
        st.session_state.extracted_files = {}
    if 'invoice_files' not in st.session_state:
        st.session_state.invoice_files = []
    if 'support_files' not in st.session_state:
        st.session_state.support_files = []
    if 'file_mapping' not in st.session_state:
        st.session_state.file_mapping = {}
    if 'user_session_id' not in st.session_state:
        st.session_state.user_session_id = str(uuid.uuid4())[:8]  # 为每个用户会话生成唯一ID
    if 'file_groups' not in st.session_state:
        st.session_state.file_groups = []
    if 'selection_changed_after_ocr' not in st.session_state:
        st.session_state.selection_changed_after_ocr = False
    if 'pending_refresh' not in st.session_state:
        st.session_state.pending_refresh = False
    if 'last_activity_type' not in st.session_state:
        st.session_state.last_activity_type = None
    if 'last_expense_type' not in st.session_state:
        st.session_state.last_expense_type = None


# 钉钉免登相关函数
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


def display_pdf(file_path):
    """使用 PyMuPDF 渲染 PDF 为高质量图像"""
    try:
        doc = fitz.open(file_path)
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))  # 2倍缩放提高清晰度
            img_data = pix.tobytes("png")
            st.image(
                img_data,
                caption=f"{os.path.basename(file_path)} - 第 {page_num + 1} 页",
                width='stretch'
            )
        doc.close()
    except Exception as e:
        st.error(f"PDF渲染失败: {str(e)}")


def display_image(file_path):
    """显示图片文件"""
    try:
        st.image(
            file_path,
            caption=os.path.basename(file_path),
            width='stretch'
        )
    except Exception as e:
        st.error(f"图片显示失败: {str(e)}")


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


def get_user_info(access_token):
    """获取用户信息 - 使用正确的API端点"""
    try:
        # 1. 获取用户基础信息（包含unionId）
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
        logging.info(f"用户基础信息: {json.dumps(me_data, indent=2)}")

        # 获取unionId作为主要标识
        union_id = me_data.get("unionId")
        if not union_id:
            logging.error("无法获取用户unionId")
            return None

        # 2. 获取服务端access_token（使用AppKey和AppSecret）
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

        # 3. 使用unionId获取用户详细信息（包含userId）
        user_url = "https://oapi.dingtalk.com/topapi/user/getbyunionid"
        user_params = {
            "access_token": corp_access_token,
            "unionid": union_id
        }
        user_response = requests.get(user_url, params=user_params, timeout=30)
        user_response.raise_for_status()
        user_data = user_response.json()

        if 'result' not in user_data:
            logging.error(f"用户信息API返回格式异常: {user_data}")
            return None

        user_info = user_data['result']
        logging.info(f"用户详细信息: {json.dumps(user_info, indent=2)}")

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


def handle_dingtalk_login():
    """处理钉钉免登流程"""
    try:
        # 检查URL参数中是否有授权码
        code = st.query_params.get("code")

        if st.session_state.dingtalk_user:
            return True

        elif code:
            # 使用授权码获取访问令牌
            with st.spinner("🔒 正在验证登录信息..."):
                access_token, expire_in = get_access_token(code)

                if access_token:
                    st.session_state.access_token = access_token

                    # 获取用户信息
                    user_info = get_user_info(access_token)
                    if user_info:
                        st.session_state.dingtalk_user = user_info

                        # 清除URL中的code参数
                        params = dict(st.query_params)
                        if "code" in params:
                            del params["code"]
                            st.query_params.clear()
                            st.query_params.update(params)
                        st.rerun()
                else:
                    st.error("⚠️ 登录失败，请重试")
                    return False
        else:
            # 显示登录按钮
            st.markdown("### 钉钉免登")
            st.markdown("请使用钉钉账号登录以继续")
            auth_url = get_dingtalk_auth_url()
            if auth_url:
                st.markdown(
                    f'<a href="{auth_url}" target="_blank" style="display: inline-block; padding: 0.8rem 1.5rem; background-color: #0086FA; color: white; border-radius: 8px; font-weight: 600; text-decoration: none; transition: all 0.3s;">'
                    '🔒 钉钉账号登录'
                    '</a>',
                    unsafe_allow_html=True
                )

            return False

        return True
    except Exception as e:
        st.error(f"登录处理失败: {str(e)}")
        return False


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

    def get_approval_instances(self, process_code):
        """获取上周所有审批实例"""
        # 获取当前时间
        now = datetime.now()

        # 减去1000天
        hundreds_days_ago = now - timedelta(days=100)

        # 将时间转换为时间戳（毫秒）
        start_time = int(hundreds_days_ago.timestamp() * 1000)

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

        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            if response.status_code == 200:
                return response.json()['result']['list']
            else:
                print(f"获取审批详情失败: {response.status_code}, {response.text}")
        except Exception as e:
            print(f"获取审批详情时出错: {str(e)}")
            return None

    def get_travel_approval_instances(self, process_code):
        """获取上周所有审批实例"""
        # 获取当前时间
        now = datetime.now()

        # 减去1000天
        hundreds_days_ago = now - timedelta(days=60)

        # 将时间转换为时间戳（毫秒）
        start_time = int(hundreds_days_ago.timestamp() * 1000)

        url = "https://api.dingtalk.com/v1.0/workflow/processes/instanceIds/query"

        payload = json.dumps({
            "startTime": start_time,
            "processCode": process_code,
            "nextToken": 0,
            "maxResults": 20,
            "userIds": [self.dd_user_id],
            "statuses": ["COMPLETED"]
        })
        headers = {
            'x-acs-dingtalk-access-token': self.access_token,
            'Content-Type': 'application/json'
        }
        instances = []
        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            if response.status_code == 200:
                return response.json()['result']['list']
            else:
                print(f"获取审批详情失败: {response.status_code}, {response.text}")
        except Exception as e:
            print(f"获取审批详情时出错: {str(e)}")
            return None

    def get_traval_application(self, process_code):
        """获取销售项目列表（示例）"""
        instances = self.get_travel_approval_instances(process_code)
        projectlist = []
        if instances is not None:
            for instance in instances:
                detail = self.get_approval_detail(instance)
                resultdict = convert_dict1_to_dict2(detail)
                projectlist.append(resultdict)
            return projectlist
        else:
            return None

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
        if not self.dd_user_id or 'dingtalk_dept' not in st.session_state:
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

    def get_approval_detail(self, instance_id):
        """获取单个审批实例的详细信息"""
        url = "https://api.dingtalk.com/v1.0/workflow/processInstances?processInstanceId=" + instance_id
        payload = {}
        headers = {
            'x-acs-dingtalk-access-token': self.access_token
        }

        try:
            response = requests.request("GET", url, headers=headers, data=payload)
            if response.status_code == 200:
                return response.json()['result']
            else:
                print(f"获取审批详情失败: {response.status_code}, {response.text}")
                return None
        except Exception as e:
            print(f"获取审批详情时出错: {str(e)}")
            return None

    def get_project_list(self, process_code):
        """获取销售项目列表（示例）"""
        instances = self.get_approval_instances(process_code)

        projectlist = []
        if instances is not None:
            for instance in instances:
                detail = self.get_approval_detail(instance)
                formdetails = detail["formComponentValues"]
                for item in formdetails:
                    if item["name"] == "项目名称":
                        projectlist.append(item["value"])
            return projectlist
        else:
            return None


def convert_dict1_to_dict2(dict1):
    # 时间字符串转换为毫秒时间戳（假设秒数为0）
    def time_str_to_timestamp(time_str):
        try:
            dt = datetime.strptime(time_str, '%Y-%m-%dT%H:%MZ')
            return int(dt.timestamp() * 1000)
        except:
            return 0

    # 格式化时间字符串（用于审批记录）
    def format_time_str(time_str):
        try:
            dt = datetime.strptime(time_str, '%Y-%m-%dT%H:%MZ')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return time_str

    # 解析行程表中的时间字符串为时间戳
    def parse_travel_time(time_str):
        if '上午' in time_str:
            date_str = time_str.replace(' 上午', '')
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            return int(dt.timestamp() * 1000)
        elif '下午' in time_str:
            date_str = time_str.replace(' 下午', '')
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            # 下午12:00:00 UTC
            return int((dt.timestamp() + 12 * 3600) * 1000)
        else:
            try:
                dt = datetime.strptime(time_str, '%Y-%m-%d')
                return int(dt.timestamp() * 1000)
            except:
                return 0

    # 提取formComponentValues中的值
    form_values = {}
    for item in dict1.get('formComponentValues', []):
        biz_alias = item.get('bizAlias')
        if biz_alias:
            form_values[biz_alias] = item.get('value', '')
        # 同时存储整个item用于后续提取
        form_values[item.get('id')] = item

    # 解析行程表（TableField）
    itinerary_data = []
    itinerary_str = form_values.get('itinerary', '[]')
    try:
        itinerary_list = json.loads(itinerary_str)
        for row in itinerary_list:
            row_value = row.get('rowValue', [])
            item_dict = {}
            for field in row_value:
                biz_alias = field.get('bizAlias')
                value = field.get('value')
                if biz_alias == 'vehicle':
                    item_dict['交通工具'] = value
                elif biz_alias == 'singleOrReturn':
                    item_dict['单程往返'] = value
                elif biz_alias == 'departure':
                    item_dict['出发城市'] = value
                elif biz_alias == 'arrival':
                    item_dict['目的城市'] = value
                elif biz_alias == 'startTime':
                    # 转换为标准时间字符串
                    ts = parse_travel_time(value)
                    dt_utc = datetime.utcfromtimestamp(ts / 1000)
                    item_dict['开始时间'] = dt_utc.strftime('%Y-%m-%d %H:%M:%S')
                elif biz_alias == 'endTime':
                    ts = parse_travel_time(value)
                    dt_utc = datetime.utcfromtimestamp(ts / 1000)
                    item_dict['结束时间'] = dt_utc.strftime('%Y-%m-%d %H:%M:%S')
                elif biz_alias == 'duration':
                    item_dict['时长'] = float(value) if value else 0.0
            itinerary_data.append(item_dict)
    except:
        itinerary_data = []

    # 构建审批单链接（使用假 corpId）
    instance_id = None
    for task in dict1.get('tasks', []):
        pc_url = task.get('pcUrl', '')
        if 'procInsId=' in pc_url:
            instance_id = pc_url.split('procInsId=')[1].split('&')[0]
            break
    corp_id = 'ding433e60cb9a4bb3bca39a90f97fcb1e09'  # 假定的 corpId
    approval_link = f'https://applink.dingtalk.com/approval/detail?corpId={corp_id}&instanceId={instance_id}&from=applink' if instance_id else ''

    alltripsdata = ''
    for each in itinerary_data:
        alltripsdata = alltripsdata + each.get('出发城市', '') + "->" + each.get('目的城市', '') + " "

    # 构建 dict2
    dict2 = {
        '提交时间': time_str_to_timestamp(dict1.get('createTime', '')),
        # '单程往返.行程': next((item.get('单程往返', '') for item in itinerary_data), '') ,
        # '交通工具.行程': next((item.get('交通工具', '') for item in itinerary_data), '') ,

        '开始时间.行程': parse_travel_time(next(
            (field.get('value') for field in json.loads(itinerary_str)[0]['rowValue'] if
             field.get('bizAlias') == 'startTime'), '')),
        '出行人（同行人）': form_values.get('traveler', ''),
        '审批完成时间': time_str_to_timestamp(dict1.get('finishTime', '')),
        # '审批状态': '已完成' if dict1.get('status') == 'COMPLETED' else '未完成',
        # '审批结果': '同意' if dict1.get('result') == 'agree' else '拒绝',
        '更新时间': time_str_to_timestamp(dict1.get('finishTime', '')),
        # '当前审批人（人员）': current_approvers,
        '行程': json.dumps(itinerary_data, ensure_ascii=False),
        '审批编号': dict1.get('businessId', ''),
        '时长.行程': form_values.get('days', ''),
        '出差事由': form_values.get('reason', ''),
        # '审批记录': approval_record_str,
        '出差天数': form_values.get('days', ''),
        '出发城市.行程': alltripsdata,  # next((item.get('出发城市', '') for item in itinerary_data), ''),
        # '目的城市.行程': alltripsdatatrans, #next((item.get('目的城市', '') for item in itinerary_data), ''),
        # '提交人（人员）': submitters,
        '审批单': {
            'link': approval_link,
            'text': '查看审批单'
        },
        # '历史审批人（人员）': history_approvers,
        # 'TableField-J8TW2TVTauto_id': '0',  # 硬编码
        '结束时间.行程': parse_travel_time(next(
            (field.get('value') for field in json.loads(itinerary_str)[0]['rowValue'] if
             field.get('bizAlias') == 'endTime'), '')),
        # '部门名称': dict1.get('originatorDeptName', '')
    }
    return dict2


# OCR服务函数
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

        if all_results:
            output_file = "invoice_results.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, ensure_ascii=False, indent=2)
            logging.info(f"结果已保存到: {output_file}")
        return all_results

    except Exception as e:
        st.error(f"票据识别出错: {str(e)}")
        return []


# 构建表格数据
def build_table_data(file_dicts, df):
    """构建钉钉动态表格所需的数据结构"""
    result_list = []
    for idx, row in df.iterrows():
        file_name = row["文件"]
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
                    "value": row.get("进项税类型", "增值税专用发票")
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


def get_default_expense_data(activity_type):
    """提供默认的费用类型数据，当Excel文件不可用时使用"""
    all_expense_types = [
        {"编码": "1464.01", "名称": "运输费", "全名": "合同履约成本-运输费", "核算维度": "项目", "余额方向": "借",
         "业务活动类型": "产品交付"},
        {"编码": "1464.02", "名称": "装卸费", "全名": "合同履约成本-装卸费", "核算维度": "项目", "余额方向": "借",
         "业务活动类型": "产品交付"},
        {"编码": "1464.03", "名称": "快递费", "全名": "合同履约成本-快递费", "核算维度": "项目", "余额方向": "借",
         "业务活动类型": "产品交付"},
        {"编码": "1464.04", "名称": "包装费", "全名": "合同履约成本-包装费", "核算维度": "项目", "余额方向": "借",
         "业务活动类型": "产品交付"},
        {"编码": "1464.06", "名称": "专项差旅费", "全名": "合同履约成本-专项差旅费", "核算维度": "项目",
         "余额方向": "借", "业务活动类型": "产品交付"},
        {"编码": "5101.06", "名称": "差旅费", "全名": "制造费用_差旅费", "核算维度": "部门", "余额方向": "借",
         "业务活动类型": "生产制造"},
        {"编码": "5101.07", "名称": "办公费", "全名": "制造费用_办公费", "核算维度": "部门", "余额方向": "借",
         "业务活动类型": "生产制造"},
        {"编码": "5101.08", "名称": "通讯费", "全名": "制造费用_通讯费", "核算维度": "部门", "余额方向": "借",
         "业务活动类型": "生产制造"},
        {"编码": "5101.12", "名称": "维修费", "全名": "制造费用_维修费", "核算维度": "部门", "余额方向": "借",
         "业务活动类型": "生产制造"},
        {"编码": "5101.14", "名称": "机物料消耗费", "全名": "制造费用_机物料消耗费", "核算维度": "部门",
         "余额方向": "借", "业务活动类型": "生产制造"},
        {"编码": "5101.15", "名称": "劳动保护费", "全名": "制造费用_劳动保护费", "核算维度": "部门", "余额方向": "借",
         "业务活动类型": "生产制造"},
        {"编码": "5101.99", "名称": "其他", "全名": "制造费用_其他", "核算维度": "部门", "余额方向": "借",
         "业务活动类型": "生产制造"},
    ]

    expense_types_by_activity = {}
    matched_expense_types = []

    for expense in all_expense_types:
        activity = expense["业务活动类型"]
        if activity not in expense_types_by_activity:
            expense_types_by_activity[activity] = []
        if expense["名称"] not in expense_types_by_activity[activity]:
            expense_types_by_activity[activity].append(expense["名称"])

        if activity == activity_type:
            matched_expense_types.append(expense)

    return expense_types_by_activity, matched_expense_types


# 智能推荐费用类型的函数
def get_suggested_expense_type(ticket_type, seller_name, activity_type):
    """根据票据类型和销售方名称智能推荐费用类型"""
    ticket_type = str(ticket_type).lower() if ticket_type else ""
    seller_name = str(seller_name).lower() if seller_name else ""

    mapping = {
        "火车": "差旅费",
        "飞机": "差旅费",
        "机票": "差旅费",
        "出租车": "交通费",
        "住宿": "住宿费",
        "酒店": "住宿费",
        "餐饮": "业务招待费",
        "饭店": "业务招待费",
        "快递": "快递费",
        "运输": "运输费",
        "包装": "包装费",
        "装卸": "装卸费",
        "维修": "维修费",
        "检测": "检测费",
        "咨询": "咨询费",
        "设计": "设计费"
    }

    if activity_type == "产品交付":
        mapping.update({
            "火车": "专项差旅费",
            "飞机": "专项差旅费",
            "机票": "专项差旅费",
            "差旅": "专项差旅费"
        })

    for key, value in mapping.items():
        if key in ticket_type:
            return value

    for key, value in mapping.items():
        if key in seller_name:
            return value

    allowed_types, _ = get_allowed_expense_types(activity_type)
    if allowed_types:
        return allowed_types[0]

    return "其他费用"


# 存储已处理文件的哈希值
processed_files = {}


def determine_user_roles(user_info):
    roles = []
    dept_name = user_info.get('dept_name', '').lower()

    if '财务' in dept_name or '会计' in dept_name:
        roles.append('财务')
    elif '综合' in dept_name:
        roles.append('IT')
    if '总监' in user_info.get('title', ''):
        roles.append('总监')
    if not roles:
        roles.append('普通员工')

    return roles


# 数据库相关函数
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


# 显示用户信息
def display_user_info():
    """显示用户信息在右上角"""
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

                if st.session_state.dingtalk_dept:
                    dept_info = st.session_state.dingtalk_dept
                    st.markdown(f"**部门**: {dept_info.get('full_path', dept_info.get('name', ''))}")
                    st.markdown(f"**部门ID**: `{dept_info.get('dept_id', '')}`")

                    with st.expander("部门详情"):
                        st.json(dept_info)
                else:
                    st.markdown(f"**部门**: `{user_info.get('dept_name', '')}`")

        balance = get_user_balance(user_info.get('userid', ''))
        if balance:
            st.write(f"备用金余额: {balance}")

        if st.button("退出登录"):
            st.session_state.dingtalk_user = None
            st.session_state.access_token = None
            st.rerun()


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


# 更精确的费用类型映射表
EXPENSE_TYPE_MAPPING = {
    "火车票": "差旅费",
    "机票": "差旅费",
    "出租车票": "交通费",
    "住宿费": "住宿费",
    "餐饮": "业务招待费",
    "办公用品": "办公费",
    "会议费": "会议费",
    "培训费": "培训费",
    "运输服务": "运输费",
    "通行费": "通行费",
    "快递服务": "快递费",
    "维修服务": "维修费",
    "咨询服务": "咨询费",
    "设计服务": "设计费"
}


def get_default_expense_type(ticket_type, activity_type):
    """根据票据类型和业务活动类型获取默认费用类型"""
    ticket_type_str = str(ticket_type).lower() if ticket_type else ""

    mapping = {
        "火车票": "专项差旅费",
        "飞机票": "差旅费",
        "机票": "差旅费",
        "出租车票": "交通费",
        "住宿": "住宿费",
        "餐饮": "业务招待费",
        "快递": "快递费",
        "运输": "运输费",
        "包装": "包装费",
        "装卸": "装卸费"
    }

    if activity_type == "产品交付":
        mapping.update({
            "火车票": "专项差旅费",
            "差旅": "专项差旅费",
            "运输": "运输费",
            "快递": "快递费",
            "包装": "包装费",
            "装卸": "装卸费"
        })
    elif activity_type in ["研发费用化", "研发资本化"]:
        mapping.update({
            "火车票": "差旅费",
            "差旅": "差旅费",
            "会议": "会议费",
            "咨询": "咨询费",
            "试验": "试验费"
        })

    for key, value in mapping.items():
        if key.lower() in ticket_type_str:
            return value

    return "其他费用"


def recommend_expense_type(ticket_type, activity_type):
    """根据票据类型和业务活动类型智能推荐费用类型"""
    default_mapping = {
        "火车票": "差旅费",
        "机票": "差旅费",
        "出租车票": "交通费",
        "住宿费": "住宿费",
        "餐饮": "业务招待费"
    }

    activity_mapping = {
        "产品交付": {
            "火车票": "专项差旅费",
            "机票": "专项差旅费",
            "运输服务": "运输费",
            "快递服务": "快递费"
        },
        "研发费用化": {
            "会议费": "会议费",
            "咨询服务": "咨询费",
            "试验费": "试验费"
        },
        "研发资本化": {
            "会议费": "会议费",
            "咨询服务": "咨询费",
            "试验费": "试验费"
        }
    }

    if activity_type in activity_mapping:
        if ticket_type in activity_mapping[activity_type]:
            return activity_mapping[activity_type][ticket_type]

    if ticket_type in default_mapping:
        return default_mapping[ticket_type]

    return EXPENSE_TYPE_MAPPING.get(ticket_type, "其他费用")


# 关联出差审批
def show_travel_application(ding):
    if not st.session_state.dingtalk_user:
        return
    data = ding.get_traval_application(DING_PROCESS_CODE_TRAVEL)
    if data == []:
        return None

    # 创建DataFrame
    df = pd.DataFrame(data)

    # 时间戳转换函数 - 只返回日期部分
    def convert_timestamp(timestamp_ms):
        try:
            # 将毫秒时间戳转换为秒
            timestamp_sec = timestamp_ms / 1000
            return datetime.fromtimestamp(timestamp_sec).strftime('%Y-%m-%d')
        except:
            return timestamp_ms

    # 转换时间戳列
    timestamp_columns = ['提交时间', '开始时间.行程', '审批完成时间', '更新时间', '结束时间.行程']
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_timestamp)

    # 初始化session_state
    if 'selected_approvals' not in st.session_state:
        st.session_state.selected_approvals = []

    # 主界面
    st.subheader("请选择要关联的审批单:")

    # 显示所有审批单的列表
    for index, row in df.iterrows():
        # 创建一行容器
        with st.container():
            cols = st.columns([2, 3, 4, 4, 3, 2, 3])

            # 第一列：复选框
            with cols[0]:
                selected = st.checkbox(
                    f"{row['提交时间']}",
                    key=f"select_{index}",
                    value=index in st.session_state.selected_approvals
                )

                # 更新选择状态
                if selected and index not in st.session_state.selected_approvals:
                    st.session_state.selected_approvals.append(index)
                elif not selected and index in st.session_state.selected_approvals:
                    st.session_state.selected_approvals.remove(index)

            # 第二列：审批编号
            with cols[1]:
                st.write(f"**{row['审批编号']}**")

            # 第三列：出差事由
            with cols[2]:
                st.write(f"{row['出差事由']}")

            # 第四列：行程信息
            with cols[3]:
                # st.write(f"📍 {row['出发城市.行程']} → {row['目的城市.行程']}")
                st.write(f"📍 {row['出发城市.行程']} ")
                # st.write(f"📅 {row['开始时间.行程']} 至 {row['结束时间.行程']}")

            # 第五列：出行人和天数
            with cols[4]:
                st.write(f"👥 {row['出行人（同行人）']}")

            # 第五列：出行人和天数
            with cols[5]:
                st.write(f"⏱️ {row['出差天数']}天")

            # 第六列：查看审批单链接
            with cols[6]:
                st.markdown(f"[查看审批单]({row['审批单']['link']})", unsafe_allow_html=True)

            # 添加分隔线
            st.divider()

    if st.button("确认选择", key="confirm_button"):
        if st.session_state.selected_approvals:
            st.success("已确认选择的审批单")
            # 这里可以添加处理选中审批单的代码
        else:
            st.warning("请先选择至少一个审批单")


def save_uploaded_file(uploaded_file):
    """保存上传的文件到临时目录"""
    try:
        # 创建临时目录，使用用户会话ID避免冲突
        temp_dir = tempfile.mkdtemp(prefix=f"user_{st.session_state.user_session_id}_")
        file_path = os.path.join(temp_dir, uploaded_file.name)

        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        return file_path
    except Exception as e:
        st.error(f"保存文件失败: {str(e)}")
        return None


def remove_file(filename):
    try:
        # 从临时文件中删除
        if filename in st.session_state.temp_files:
            file_path = st.session_state.temp_files[filename]
            if os.path.exists(file_path):
                os.remove(file_path)
                # 尝试删除空目录
                dir_path = os.path.dirname(file_path)
                if os.path.exists(dir_path) and not os.listdir(dir_path):
                    try:
                        os.rmdir(dir_path)
                    except:
                        pass
            del st.session_state.temp_files[filename]

        # ... 其他清理逻辑
    except Exception as e:
        st.error(f"删除文件失败: {str(e)}")
        logging.error(f"删除文件失败: {str(e)}")


def display_file_preview(filename, file_path):
    """显示文件预览"""
    try:
        file_ext = os.path.splitext(filename)[1].lower()

        if file_ext == '.pdf':
            display_pdf(file_path)
        elif file_ext in ['.jpg', '.jpeg', '.png', '.bmp', '.gif']:
            display_image(file_path)
        else:
            st.warning(f"不支持预览的文件类型: {file_ext}")
    except Exception as e:
        st.error(f"预览文件失败: {str(e)}")


def extract_archive(uploaded_file, extract_to):
    """解压上传的压缩文件到指定目录"""
    try:
        # 确保目标目录存在
        Path(extract_to).mkdir(parents=True, exist_ok=True)

        # 获取文件名并转换为小写以进行扩展名检查
        filename = uploaded_file.name.lower()

        # 根据文件扩展名选择解压方法
        if filename.endswith('.zip'):
            with zipfile.ZipFile(uploaded_file, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            return True, f"成功解压 ZIP 文件到 {extract_to}"

        elif filename.endswith(('.tar', '.tar.gz', '.tgz')):
            with tarfile.open(fileobj=uploaded_file, mode='r:*') as tar_ref:
                tar_ref.extractall(extract_to)
            return True, f"成功解压 TAR 文件到 {extract_to}"

        else:
            return False, "不支持的压缩格式，请上传 ZIP 或 TAR 文件"

    except Exception as e:
        return False, f"解压过程中出错: {str(e)}"


def classify_files(extract_dir):
    """根据文件名规则分类文件"""
    invoice_files = []
    support_files = []
    file_mapping = {}

    try:
        # 遍历解压目录中的所有文件
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, extract_dir)

                # 根据文件名规则分类
                if re.match(r'^\d+_01_', file):
                    # 发票文件
                    invoice_files.append({
                        'path': file_path,
                        'name': file,
                        'relative_path': relative_path
                    })

                    # 提取前缀（数字部分）
                    prefix = re.match(r'^(\d+)_', file).group(1)
                    if prefix not in file_mapping:
                        file_mapping[prefix] = {'invoice': [], 'support': []}
                    file_mapping[prefix]['invoice'].append(file_path)

                elif re.match(r'^\d+_02_', file):
                    # 支持文件
                    support_files.append({
                        'path': file_path,
                        'name': file,
                        'relative_path': relative_path
                    })

                    # 提取前缀（数字部分）
                    prefix = re.match(r'^(\d+)_', file).group(1)
                    if prefix not in file_mapping:
                        file_mapping[prefix] = {'invoice': [], 'support': []}
                    file_mapping[prefix]['support'].append(file_path)

        # 按文件名排序
        invoice_files.sort(key=lambda x: x['name'])
        support_files.sort(key=lambda x: x['name'])

        return invoice_files, support_files, file_mapping

    except Exception as e:
        logging.error(f"文件分类失败: {str(e)}")
        return [], [], {}


def process_uploaded_archive(uploaded_file):
    """处理上传的压缩文件"""
    try:
        # 创建临时目录用于解压，使用用户会话ID避免冲突
        temp_dir = tempfile.mkdtemp(prefix=f"user_{st.session_state.user_session_id}_")
        extract_dir = os.path.join(temp_dir, 'extracted')

        # 解压文件
        success, message = extract_archive(uploaded_file, extract_dir)
        if not success:
            return False, message, [], [], {}

        # 分类文件
        invoice_files, support_files, file_mapping = classify_files(extract_dir)

        # 创建文件组
        file_groups = create_file_groups(invoice_files, support_files, file_mapping)

        # 保存到session state
        st.session_state.extracted_files = {
            'temp_dir': temp_dir,
            'extract_dir': extract_dir
        }
        st.session_state.invoice_files = invoice_files
        st.session_state.support_files = support_files
        st.session_state.file_mapping = file_mapping
        st.session_state.file_groups = file_groups

        return True, "文件解压和分类成功", invoice_files, support_files, file_mapping

    except Exception as e:
        return False, f"处理压缩文件失败: {str(e)}", [], [], {}


def create_file_groups(invoice_files, support_files, file_mapping):
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


def update_table_with_new_selections(df, activity_type, expense_type, project_name):
    """更新表格中的业务活动类型、费用类型和项目名称"""
    """更新表格中的业务活动类型、费用类型和项目名称"""
    if df is not None and not df.empty:
        df = df.copy()  # 创建副本
        df["业务活动类型"] = activity_type
        df["费用类型"] = expense_type  # 确保费用类型被更新
        if project_name:
            df["项目名称"] = project_name

        # 记录更新日志
        logging.info(f"表格已更新 - 活动类型: {activity_type}, 费用类型: {expense_type}, 项目: {project_name}")

    return df


# 修改文件显示部分，将文件列表和预览合并
def display_file_preview_combined(filename, file_path):
    """合并显示文件信息和预览"""
    try:
        file_ext = os.path.splitext(filename)[1].lower()

        # 创建可折叠区域，标题包含文件信息
        with st.expander(f"📄 {filename}", expanded=False):
            col1, col2 = st.columns([3, 1])

            with col1:
                # 显示文件预览
                if file_ext == '.pdf':
                    display_pdf(file_path)
                elif file_ext in ['.jpg', '.jpeg', '.png', '.bmp', '.gif']:
                    display_image(file_path)
                else:
                    st.warning(f"不支持预览的文件类型: {file_ext}")

            with col2:
                if not st.session_state.ocr_processed:
                    delete_key = f"delete_{hash(filename)}"
                    if st.button("删除", key=delete_key):
                        # 这里可以添加删除逻辑
                        st.warning("删除功能暂未实现")
                else:
                    st.info("文件已处理")
    except Exception as e:
        st.error(f"预览文件失败: {str(e)}")

# 主函数
def main():
    # 初始化session状态
    init_session_state()

    # 显示标题
    st.markdown('<h1 class="main-header">智能票据审批系统</h1>', unsafe_allow_html=True)

    # 处理钉钉免登
    if not handle_dingtalk_login():
        return

    # 显示用户信息
    if st.session_state.dingtalk_user:
        display_user_info()
        show_advance_fund_query()
        show_expense_types()
        ding = DingTalkApproval()
        show_travel_application(ding)

        # 初始化全局选择状态
        if 'global_activity_type' not in st.session_state:
            st.session_state.global_activity_type = None
        if 'global_project_name' not in st.session_state:
            st.session_state.global_project_name = None
        if 'selected_expense_type' not in st.session_state:
            st.session_state.selected_expense_type = None
        if 'expense_full_name' not in st.session_state:
            st.session_state.expense_full_name = None
        if 'expense_details' not in st.session_state:
            st.session_state.expense_details = []

        # 项目选择
        project_name = ""
        # 业务活动类型选择
        activity_type = st.selectbox(
            "1. 请选择本次报销的业务活动类型 *",
            options=["产品交付", "生产制造", "研发费用化", "研发资本化", "销售费用", "管理费用"],
            index=0,
            placeholder="请选择业务活动类型",
            key="activity_type_select"
        )

        # 费用类型选择 - 在选择业务活动类型后显示
        if activity_type:
            allowed_expense_types, expense_details = get_allowed_expense_types(activity_type)
            st.session_state.expense_details = expense_details

            if allowed_expense_types:
                # 确保费用类型在允许的列表中
                current_expense_type = st.session_state.selected_expense_type
                if current_expense_type not in allowed_expense_types:
                    # 如果当前费用类型不在新业务活动类型的允许列表中，重置为第一个选项
                    current_expense_type = allowed_expense_types[0]
                    st.session_state.selected_expense_type = current_expense_type
                    st.info(f"费用类型已自动更新为: {current_expense_type}")

                expense_type = st.selectbox(
                    "2. 请选择费用类型 *",
                    options=allowed_expense_types,
                    index=allowed_expense_types.index(
                        current_expense_type) if current_expense_type in allowed_expense_types else 0,
                    placeholder="请选择费用类型",
                    key="expense_type"
                )
                st.session_state.selected_expense_type = expense_type

                # 获取选中的费用类型的全名
                if expense_type and expense_details:
                    for detail in expense_details:
                        if detail["名称"] == expense_type:
                            st.session_state.expense_full_name = detail["全名"]
                            break
            else:
                st.error(f"当前业务活动类型 '{activity_type}' 没有配置费用类型，请联系管理员")
                return

        if activity_type:
            if activity_type == "产品交付":
                sales_projects = ding.get_project_list(DING_PROCESS_CODE_MARKET)
                if sales_projects is not None:
                    project_name = st.selectbox(
                        "请选择产品交付关联的销售项目 *",
                        options=sales_projects,
                        index=0,
                        placeholder="请选择销售项目",
                        key="sales_project"
                    )
                else:
                    st.error(
                        "当前所有钉钉应用调用该接口次数过多，超出了该接口承受的最大qps，请求被暂时限制了。请稍后再试。")
            elif activity_type in ["研发费用化", "研发资本化"]:
                rd_projects = ding.get_project_list(DING_PROCESS_CODE_RD)
                if rd_projects is not None:
                    project_name = st.selectbox(
                        "请选择研发活动关联的研发项目*",
                        options=rd_projects,
                        index=0,
                        placeholder="请选择研发项目",
                        key="rd_project"
                    )
                else:
                    st.error(
                        "当前所有钉钉应用调用该接口次数过多，超出了该接口承受的最大qps，请求被暂时限制了。请稍后再试。")

        # 检查选择是否发生变化 - 改进版本
        activity_changed = st.session_state.global_activity_type != activity_type
        project_changed = st.session_state.global_project_name != project_name
        expense_changed = st.session_state.selected_expense_type != expense_type

        selection_changed = activity_changed or project_changed or expense_changed

        # 处理选择变化
        if selection_changed:
            # 如果已经提交过审批，重置所有状态
            if st.session_state.approval_submitted:
                # 清理临时文件
                if st.session_state.extracted_files:
                    temp_dir = st.session_state.extracted_files.get('temp_dir')
                    if temp_dir and os.path.exists(temp_dir):
                        try:
                            shutil.rmtree(temp_dir)
                        except Exception as e:
                            logging.error(f"清理临时目录失败: {str(e)}")

                # 重置所有状态
                st.session_state.uploaded_files = []
                st.session_state.all_ocr_results = []
                st.session_state.processed_files = {}
                st.session_state.ocr_processed = False
                st.session_state.editable_df = None
                st.session_state.temp_files = {}
                st.session_state.revision_confirmed = False
                st.session_state.selected_approvals = []
                st.session_state.uploader_key += 1
                st.session_state.approval_submitted = False
                st.session_state.approval_instance_id = None
                st.session_state.extracted_files = {}
                st.session_state.invoice_files = []
                st.session_state.support_files = []
                st.session_state.file_mapping = {}
                st.session_state.file_groups = []
                st.session_state.selection_changed_after_ocr = False
                st.success("已重置状态，请重新上传文件开始新的报销流程")
                st.rerun()


            # 如果已经识别了票据但未提交审批，更新表格而不重置
            elif st.session_state.ocr_processed and st.session_state.editable_df is not None:
                # 立即更新表格中的相关字段
                st.session_state.editable_df = update_table_with_new_selections(
                    st.session_state.editable_df,
                    activity_type,
                    expense_type,
                    project_name
                )
                st.session_state.selection_changed_after_ocr = True
                # 特别处理费用类型变化的情况
                if expense_changed:
                    st.success(f"费用类型已更新为: {expense_type}")
                # 存储全局选择
                st.session_state.global_activity_type = activity_type
                st.session_state.global_project_name = project_name
                st.session_state.selected_expense_type = expense_type

                st.success("已更新表格中的业务活动类型和费用类型")
                st.rerun()
            # 如果还没有识别票据，则重置上传状态
            else:
                # 清理临时文件
                if st.session_state.extracted_files:
                    temp_dir = st.session_state.extracted_files.get('temp_dir')
                    if temp_dir and os.path.exists(temp_dir):
                        try:
                            shutil.rmtree(temp_dir)
                        except Exception as e:
                            logging.error(f"清理临时目录失败: {str(e)}")

                st.session_state.uploaded_files = []
                st.session_state.all_ocr_results = []
                st.session_state.processed_files = {}
                st.session_state.ocr_processed = False
                st.session_state.editable_df = None
                st.session_state.extracted_files = {}
                st.session_state.invoice_files = []
                st.session_state.support_files = []
                st.session_state.file_mapping = {}
                st.session_state.file_groups = []
                st.session_state.selection_changed_after_ocr = False

                # 存储全局选择
                st.session_state.global_activity_type = activity_type
                st.session_state.global_project_name = project_name
                st.session_state.selected_expense_type = expense_type

                st.rerun()



        # 验证业务活动类型和项目
        activity_valid = True
        if not activity_type:
            activity_valid = False
        elif activity_type in ["产品交付", "研发费用化", "研发资本化"] and not project_name:
            activity_valid = False
        elif not st.session_state.selected_expense_type:
            activity_valid = False

        # 存储全局选择
        if activity_valid:
            st.session_state.global_activity_type = activity_type
            st.session_state.global_project_name = project_name
            st.session_state.selected_expense_type = expense_type

        # 只有业务活动类型和项目有效时才显示文件上传
        if activity_valid:

            st.info(
                "3. 请上传包含所有票据文件的压缩文件。文件命名规则：发票文件为0N_01_xxx，支持文件为0N_02_xxx（N为数字）")

            # 文件上传组件 - 单个压缩文件
            uploaded_file = st.file_uploader(
                "选择压缩文件（包含所有票据文件和支持文件）",
                type=['zip', 'tar', 'tar.gz', 'tgz'],
                help="支持格式: ZIP, TAR, TAR.GZ",
                key=f'file_uploader_{st.session_state.uploader_key}'
            )

            # 处理新上传的文件
            if uploaded_file and uploaded_file not in st.session_state.uploaded_files:
                # 清理之前的临时文件
                if st.session_state.extracted_files:
                    temp_dir = st.session_state.extracted_files.get('temp_dir')
                    if temp_dir and os.path.exists(temp_dir):
                        try:
                            shutil.rmtree(temp_dir)
                        except Exception as e:
                            logging.error(f"清理临时目录失败: {str(e)}")

                st.session_state.uploaded_files = [uploaded_file]
                with st.spinner("正在解压和分类文件..."):
                    success, message, invoice_files, support_files, file_mapping = process_uploaded_archive(
                        uploaded_file)

                if success:
                    st.success(message)

                #     # 显示文件分类结果
                #     st.subheader("📋 报销明细")
                #
                #     # 显示文件组
                #     for group in st.session_state.file_groups:
                #         with st.expander(f"📁 报销明细 {group['prefix']}", expanded=True):
                #             col1, col2 = st.columns(2)
                #
                #             with col1:
                #                 st.write("**📄 发票文件:**")
                #                 for invoice_file in group['invoice_files']:
                #                     st.write(f"✅ {invoice_file['name']}")
                #
                #             with col2:
                #                 st.write("**📎 支持文件:**")
                #                 for support_file in group['support_files']:
                #                     st.write(f"📎 {support_file['name']}")
                #
                # else:
                #     st.error(message)

            # 显示已上传文件列表
            if st.session_state.uploaded_files and st.session_state.file_groups:
                custom_subheader("已上传文件", font_size=16, color='black')

                # 显示所有文件组
                for group in st.session_state.file_groups:
                    st.write(f"**报销明细 {group['prefix']}:**")

                    # 显示发票文件
                    for invoice_file in group['invoice_files']:
                        display_file_preview_combined(invoice_file['name'], invoice_file['path'])

                    # 显示支持文件
                    for support_file in group['support_files']:
                        display_file_preview_combined(support_file['name'], support_file['path'])

                    # # 显示发票文件
                    # for invoice_file in group['invoice_files']:
                    #     with st.expander(f"📄 {invoice_file['name']}   （点开预览）", expanded=False):
                    #         col1, col2 = st.columns([3, 1])
                    #
                    #         with col1:
                    #             # 显示文件预览
                    #             display_file_preview(invoice_file['name'], invoice_file['path'])
                    #
                    #         with col2:
                    #             if not st.session_state.ocr_processed:
                    #                 delete_key = f"delete_{hash(invoice_file['name'])}"
                    #                 if st.button("删除", key=delete_key):
                    #                     # 这里可以添加删除逻辑
                    #                     st.warning("删除功能暂未实现")
                    #             else:
                    #                 st.info("文件已处理")
                    #
                    # # 显示支持文件
                    # for support_file in group['support_files']:
                    #     with st.expander(f"📎 {support_file['name']}   （点开预览）", expanded=False):
                    #         col1, col2 = st.columns([3, 1])
                    #
                    #         with col1:
                    #             # 显示文件预览
                    #             display_file_preview(support_file['name'], support_file['path'])
                    #
                    #         with col2:
                    #             if not st.session_state.ocr_processed:
                    #                 delete_key = f"delete_{hash(support_file['name'])}"
                    #                 if st.button("删除", key=delete_key):
                    #                     # 这里可以添加删除逻辑
                    #                     st.warning("删除功能暂未实现")
                    #             else:
                    #                 st.info("文件已处理")

                # 添加识别按钮
                if not st.session_state.ocr_processed and st.session_state.invoice_files:
                    if st.button("开始识别票据", type="primary"):
                        with st.spinner("正在识别票据，请稍候..."):
                            # 只对发票文件进行OCR识别
                            invoice_file_paths = [file_info['path'] for file_info in st.session_state.invoice_files]
                            ocr_results = ocr_invoice(invoice_file_paths)

                            if ocr_results:
                                st.session_state.all_ocr_results = ocr_results
                                st.session_state.ocr_processed = True

                                # 构建包含发票和支持文件的表格数据
                                table_data = build_combined_table_data(ocr_results, st.session_state.file_mapping)

                                if table_data:
                                    df = pd.DataFrame(table_data)
                                    st.session_state.editable_df = df
                                    st.rerun()
                            else:
                                st.error("票据识别失败，请重试")
                            pass

            # 在显示可编辑表格之前，添加费用类型同步检查


            # 显示可编辑表格
            if st.session_state.ocr_processed and st.session_state.editable_df is not None:
                df = st.session_state.editable_df
                # 强制同步费用类型
                current_expense_type = st.session_state.selected_expense_type
                if current_expense_type and "费用类型" in df.columns:
                    if not df["费用类型"].equals(pd.Series([current_expense_type] * len(df))):
                        df["费用类型"] = current_expense_type
                        st.session_state.editable_df = df
                        logging.info(f"强制同步费用类型为: {current_expense_type}")

                # 调试信息 - 可选
                if st.session_state.get('debug_mode', False):
                    st.write("当前表格中的费用类型:", df["费用类型"].unique())
                    st.write("当前选择的费用类型:", st.session_state.selected_expense_type)

                # 检查是否需要显示更新提示
                if st.session_state.get('selection_changed_after_ocr', False):
                    st.info("业务活动类型或费用类型已更新，请确认表格信息")
                    # 重置标志，避免重复显示
                    st.session_state.selection_changed_after_ocr = False

                # 确保df是有效的DataFrame并且包含所需的列
                if not isinstance(df, pd.DataFrame) or df.empty:
                    st.error("数据表格无效或为空，请重新上传文件")
                    st.session_state.ocr_processed = False
                    st.session_state.editable_df = None
                    st.rerun()

                # 检查是否包含必要的列
                required_columns = ["报销含税金额", "不含进项税金额", "进项税额", "进项税类型"]
                missing_columns = [col for col in required_columns if col not in df.columns]

                if missing_columns:
                    st.error(f"数据表格缺少必要的列: {', '.join(missing_columns)}")
                    st.session_state.ocr_processed = False
                    st.session_state.editable_df = None
                    st.rerun()

                st.markdown("""
                        <style>
                        /* 调整 metric 组件的字体大小 */
                        div[data-testid="stMetricValue"] {
                            font-size: 20px !important;
                        }
                        div[data-testid="stMetricLabel"] {
                            font-size: 14px !important;
                        }
                        </style>
                    """, unsafe_allow_html=True)

                custom_subheader("票据汇总", font_size=20)

                # 计算总金额
                try:
                    total_with_tax = df["报销含税金额"].sum()
                    total_without_tax = df["不含进项税金额"].sum()
                    total_tax = df["进项税额"].sum()
                except Exception as e:
                    st.error(f"计算金额时出错: {str(e)}")
                    st.write("数据表格内容:")
                    st.write(df)
                    st.session_state.ocr_processed = False
                    st.session_state.editable_df = None
                    st.rerun()

                col1, col2, col3 = st.columns(3)
                col1.metric("报销含税金额", f"¥{total_with_tax:.2f}")
                col2.metric("不含进项税金额", f"¥{total_without_tax:.2f}")
                col3.metric("进项税额", f"¥{total_tax:.2f}")

                # 如果选择发生变化，显示提示
                if st.session_state.selection_changed_after_ocr:
                    st.info("业务活动类型或费用类型已更新，请确认表格信息")
                    st.session_state.selection_changed_after_ocr = False

                st.divider()

                # 创建一个新的DataFrame，添加序号列
                df_with_index = df.copy()
                df_with_index.insert(0, "序号", range(1, len(df) + 1))

                # 将整个 data_editor 放入一个表单中
                with st.form(key='invoice_editable_table_form', border=False):
                    custom_subheader("票据详情", font_size=20)

                    edited_df = st.data_editor(
                        df_with_index,
                        column_config={
                            "序号": st.column_config.NumberColumn(
                                "序号",
                                width="small",
                                disabled=True
                            ),
                            "文件": st.column_config.TextColumn(
                                "文件",
                                width="small",
                                disabled=True
                            ),
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
                                options=["增值税专用发票", "增值税普通发票", "其他"],
                                required=True,
                                width="small"
                            ),
                            "业务活动类型": st.column_config.TextColumn(
                                "业务活动类型",
                                disabled=True,
                                width="small"
                            ),
                            "项目名称": st.column_config.TextColumn(
                                "项目/部门",
                                disabled=True,
                                width="small"
                            ),
                            "费用类型": st.column_config.TextColumn(
                                "费用类型",
                                disabled=False,  # 允许通过程序更新
                                width="medium"
                            )
                        },
                        hide_index=True,
                        width='stretch',
                        height=min(500, 100 + len(df_with_index) * 40),
                        num_rows="fixed",
                        key="invoice_editor"
                    )

                    revisionsubmitted = st.form_submit_button('确认')

                if revisionsubmitted:
                    # 移除序号列，恢复原始数据结构
                    edited_df = edited_df.drop(columns=["序号"])
                    st.session_state.editable_df = edited_df
                    st.session_state.revision_confirmed = True  # 添加一个状态标记
                    st.success("票据信息已确认!")
                    st.rerun()

            # 在确认票据信息后显示审批表单
            if st.session_state.get('revision_confirmed', False):
                df = st.session_state.editable_df

                # 确保df是有效的DataFrame并且包含所需的列
                if not isinstance(df, pd.DataFrame) or df.empty:
                    st.error("数据表格无效或为空，请重新上传文件")
                    st.session_state.revision_confirmed = False
                    st.rerun()

                # 安全地计算总和
                try:
                    new_total = df["报销含税金额"].sum()
                except Exception as e:
                    st.error(f"计算总金额时出错: {str(e)}")
                    st.write("数据表格内容:")
                    st.write(df)
                    st.session_state.revision_confirmed = False
                    st.rerun()

                # 显示确认后的总金额
                st.markdown(f"<div style='text-align: center; margin-top: 18px; margin-bottom: 18px;'>"
                            f"<h3 style='color: blue;'>确认后报销含税金额: ¥{new_total:.2f}</h3></div>",
                            unsafe_allow_html=True)

                contains_travel = "差旅费" in st.session_state.selected_expense_type
                if contains_travel and st.session_state.selected_approvals == []:
                    custom_warning("票据包括差旅费，但是没有关联的出差审批！提交的审批可能会被退回。")

                # 查询备用金余额
                user_info = st.session_state.dingtalk_user
                user_id = user_info.get('userid', '')
                advance_balance_str = get_user_balance(user_id)

                if advance_balance_str:
                    advance_balance = float(advance_balance_str.replace('¥', '').replace(',', ''))

                    if advance_balance >= new_total:
                        st.markdown(
                            f"**有备用金：¥{advance_balance:.2f}元。审批通过后还有备用金:¥{advance_balance - new_total:.2f}元**。")
                    elif advance_balance > 0:
                        st.markdown(
                            f"**有备用金：¥{advance_balance:.2f}元。审批通过后报销：¥{new_total - advance_balance:.2f}元，再没有备用金。**")
                    else:
                        st.markdown(
                            f"**没有备用金。审批通过后报销：¥{new_total:.2f}元。**")

                # 确认和提交表单
                with st.form("approval_form", clear_on_submit=True):
                    custom_subheader("确认提交", font_size=20)
                    st.markdown(f"<h4 style='text-align: center;'>总报销金额: ¥{new_total:.2f}</h4>",
                                unsafe_allow_html=True)

                    # 使用费用类型的全名作为报销事由的默认值
                    reason = st.text_input("报销事由",
                                           value=st.session_state.get('expense_full_name', '日常费用报销'))

                    agree = st.checkbox("我确认所有票据信息正确无误", value=False)

                    # 使用 st.form_submit_button 创建提交按钮
                    submitted = st.form_submit_button("提交审批", type="primary")

                # 将提交处理逻辑移到表单外部
                if submitted:

                    if not agree:
                        st.warning("请先确认票据信息正确")
                    else:
                        try:
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
                                "total_amount_withtax": float(new_total),
                                "total_amount_withouttax": float(df["不含进项税金额"].sum()),
                                "total_amount_tax": float(df["进项税额"].sum()),
                                "activity_type": st.session_state.global_activity_type,
                                "project_name": st.session_state.global_project_name if st.session_state.global_project_name else "部门",
                                "expense_type": st.session_state.selected_expense_type,
                                "ticket_count": len(df)  # 只计算发票文件数量
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
                                st.success(f"审批单创建成功! 流程ID: {response['instanceId']}")
                                st.balloons()

                                # 设置审批成功标志
                                st.session_state.approval_submitted = True
                                st.session_state.approval_instance_id = response['instanceId']

                                # 清理临时文件
                                if st.session_state.extracted_files:
                                    temp_dir = st.session_state.extracted_files.get('temp_dir')
                                    if temp_dir and os.path.exists(temp_dir):
                                        try:
                                            shutil.rmtree(temp_dir)
                                        except Exception as e:
                                            logging.error(f"清理临时目录失败: {str(e)}")

                                # 增加 uploader_key 来刷新文件上传器
                                st.session_state.uploader_key += 1

                                st.rerun()
                            else:
                                error_msg = response.get('message', '未知错误')
                                if 'errmsg' in response:
                                    error_msg = response['errmsg']
                                elif 'error_description' in response:
                                    error_msg = response['error_description']
                                elif 'error' in response:
                                    error_msg = response['error']

                                st.error(f"提交失败: {error_msg}")
                                st.json(response)
                        except Exception as e:
                            st.error(f"提交审批过程中发生错误: {str(e)}")
                            logging.exception("提交审批异常")
    # 处理待刷新状态
    if st.session_state.get('pending_refresh', False):
        st.session_state.pending_refresh = False
        st.rerun()

if __name__ == "__main__":
    main()