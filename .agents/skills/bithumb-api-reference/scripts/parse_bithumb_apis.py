import re
import json
import urllib.request
import urllib.parse
import os
import sys

# 인코딩 관련 조치 (한글 파일명 및 한글 로깅 출력 지원)
sys.stdout.reconfigure(encoding='utf-8')

# 스크립트 위치 기준으로 상대 경로 설정 (이식성 극대화)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REF_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "references"))
PUBLIC_API_PATH = os.path.join(REF_DIR, "public_api.md")
PRIVATE_API_PATH = os.path.join(REF_DIR, "private_api.md")

os.makedirs(REF_DIR, exist_ok=True)

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

def fetch_url(url):
    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
    with urllib.request.urlopen(req) as resp:
        return resp.read()

# 1. llms.txt 에서 모든 API 레퍼런스 링크 수집
LLMS_URL = "https://apidocs.bithumb.com/llms.txt"

print(f"[*] llms.txt 인덱스 다운로드 중... ({LLMS_URL})")
try:
    llms_content = fetch_url(LLMS_URL).decode('utf-8')
except Exception as e:
    print(f"[!] llms.txt 다운로드 실패: {e}")
    sys.exit(1)

# API Reference 섹션의 마크다운 링크 추출
links = re.findall(r'-\s+\[(.*?)\]\((https://apidocs\.bithumb\.com/reference/.*?\.md)\)', llms_content)

print(f"[*] 총 {len(links)}개의 후보 레퍼런스 링크를 발견했습니다.")

# 수집 데이터 목록
public_apis = []
private_apis = []

def parse_schema_properties(schema, required_fields=None):
    if required_fields is None:
        required_fields = []
    
    props = []
    if not schema or 'properties' not in schema:
        return props
        
    for prop_name, prop_info in schema['properties'].items():
        p_type = prop_info.get('type', 'any')
        p_req = "✅ 필수" if prop_name in required_fields or prop_info.get('required', False) else "선택"
        p_desc = prop_info.get('description', '').replace('\n', ' ')
        p_example = prop_info.get('example', '')
        if p_example:
            p_desc += f" (예시: {p_example})"
        props.append({
            "name": prop_name,
            "type": p_type,
            "required": p_req,
            "description": p_desc
        })
    return props

# 2. 각 API 문서 다운로드 및 OpenAPI 명세 파싱
for title, url in links:
    print(f"[*] 다운로드 및 파싱 중: [{title}] ({url})")
    
    parsed_url = urllib.parse.urlparse(url)
    encoded_path = urllib.parse.quote(parsed_url.path)
    encoded_url = urllib.parse.urlunparse(parsed_url._replace(path=encoded_path))
    
    try:
        md_content = fetch_url(encoded_url).decode('utf-8')
    except Exception as e:
        print(f"    [!] 다운로드 실패 ({title}): {e}")
        continue

    # 마크다운 안에서 OpenAPI JSON 코드 블록 추출
    json_blocks = re.findall(r'```json\s*(\{.*?\})\s*```', md_content, re.DOTALL)
    
    if not json_blocks:
        print(f"    [i] OpenAPI JSON 명세가 존재하지 않는 가이드 문서입니다. 건너뜁니다.")
        continue
        
    # JSON 데코더 검증 시 paths 객체를 포함하는 OpenAPI 온전 스펙만 매칭하도록 수정
    api_spec = None
    for block in json_blocks:
        try:
            spec = json.loads(block.strip())
            if isinstance(spec, dict) and 'paths' in spec:
                api_spec = spec
                break
        except json.JSONDecodeError:
            continue
            
    if not api_spec:
        print(f"    [i] 유효한 OpenAPI paths 객체를 찾지 못했습니다. 건너뜁니다.")
        continue
        
    base_url = "https://api.bithumb.com"
    if 'servers' in api_spec and len(api_spec['servers']) > 0:
        base_url = api_spec['servers'][0].get('url', base_url)
        
    for path, path_info in api_spec['paths'].items():
        for method, method_info in path_info.items():
            if method.lower() not in ['get', 'post', 'put', 'delete', 'patch']:
                continue
                
            summary = method_info.get('summary', title)
            description = method_info.get('description', '')
            
            params = []
            is_private = False
            
            if 'parameters' in method_info:
                for param in method_info['parameters']:
                    p_name = param.get('name', '')
                    p_in = param.get('in', 'query')
                    p_req = "✅ 필수" if param.get('required', False) else "선택"
                    p_desc = param.get('description', '').replace('\n', ' ')
                    p_type = param.get('schema', {}).get('type', 'string')
                    
                    if p_name.lower() == 'authorization':
                        is_private = True
                        continue
                        
                    params.append({
                        "name": p_name,
                        "in": p_in,
                        "type": p_type,
                        "required": p_req,
                        "description": p_desc
                    })
            
            request_body_schema = None
            if 'requestBody' in method_info:
                content = method_info['requestBody'].get('content', {})
                for content_type, content_info in content.items():
                    if 'schema' in content_info:
                        request_body_schema = content_info['schema']
                        req_fields = request_body_schema.get('required', [])
                        body_props = parse_schema_properties(request_body_schema, req_fields)
                        for prop in body_props:
                            params.append({
                                "name": prop["name"],
                                "in": "body (json)",
                                "type": prop["type"],
                                "required": prop["required"],
                                "description": prop["description"]
                            })
                            
            response_example = None
            if 'responses' in method_info and '200' in method_info['responses']:
                res_200 = method_info['responses']['200']
                content = res_200.get('content', {})
                for ct, ct_info in content.items():
                    if 'examples' in ct_info:
                        examples = ct_info['examples']
                        first_key = list(examples.keys())[0]
                        val = examples[first_key].get('value', {})
                        if isinstance(val, str):
                            try:
                                response_example = json.loads(val)
                            except:
                                response_example = val
                        else:
                            response_example = val
                    elif 'example' in ct_info:
                        response_example = ct_info['example']
                    elif 'schema' in ct_info and 'example' in ct_info['schema']:
                        response_example = ct_info['schema']['example']
                        
            # 정규표현식을 적용하여 /orderbook이 private에 잘못 걸리는 버그 수정
            private_path_patterns = [
                r'/orders',
                r'/order(?![a-zA-Z])',
                r'/accounts',
                r'/twap',
                r'/withdraw',
                r'/deposit',
                r'/key',
                r'/api-key'
            ]
            if any(re.search(pat, path.lower()) for pat in private_path_patterns):
                is_private = True
                
            api_data = {
                "title": summary,
                "method": method.upper(),
                "path": path,
                "base_url": base_url,
                "description": description,
                "params": params,
                "response": response_example
            }
            
            if is_private:
                private_apis.append(api_data)
            else:
                public_apis.append(api_data)

def build_markdown_content(api_list, is_private_doc=False):
    doc_title = "빗썸 Private API 레퍼런스" if is_private_doc else "빗썸 Public API 레퍼런스"
    auth_desc = "Private API 호출 시에는 헤더에 JWT 인증 토큰과 파라미터 해시(`query_hash`)를 필수로 실어 보내야 합니다." if is_private_doc else "Public API는 인증(JWT) 없이 호출할 수 있는 시장 데이터 조회 API입니다."
    
    md = []
    md.append(f"# {doc_title}\n")
    md.append(f"{auth_desc}\n")
    md.append(f"* **공통 Base URL**: `https://api.bithumb.com`\n")
    md.append("---\n")
    
    md.append("## API 목록 목차\n")
    for api in api_list:
        anchor = api["title"].replace(" ", "-").replace("/", "").lower()
        md.append(f"* [{api['title']}](#{anchor}) (`{api['method']} {api['path']}`)")
    md.append("\n---\n")
    
    for api in api_list:
        anchor = api["title"].replace(" ", "-").replace("/", "").lower()
        md.append(f"## {api['title']}\n")
        if api['description']:
            md.append(f"{api['description']}\n")
            
        md.append(f"* **HTTP Request**: `{api['method']} {api['base_url']}{api['path']}`")
        if is_private_doc:
            md.append(f"* **인증 요구사항**: `Bearer JWT` (필수)")
        else:
            md.append(f"* **인증 요구사항**: 없음")
            
        md.append("\n### 요청 파라미터 명세\n")
        if not api['params']:
            md.append("보내는 파라미터가 없습니다.\n")
        else:
            md.append("| 파라미터명 | 위치 | 타입 | 필수여부 | 설명 |")
            md.append("|---|---|---|---|---|")
            for p in api['params']:
                md.append(f"| `{p['name']}` | `{p['in']}` | `{p['type']}` | {p['required']} | {p['description']} |")
            md.append("")
            
        md.append("### 응답 예시 (200 OK)\n")
        if api['response']:
            try:
                formatted_json = json.dumps(api['response'], indent=2, ensure_ascii=False)
                md.append(f"```json\n{formatted_json}\n```\n")
            except Exception:
                md.append(f"```\n{api['response']}\n```\n")
        else:
            md.append("성공 시 빈 객체 또는 표준 성공 메시지가 반환됩니다.\n")
            
        md.append("---\n")
        
    return "\n".join(md)

print(f"[*] Public API 레퍼런스 파일 쓰는 중... ({PUBLIC_API_PATH})")
with open(PUBLIC_API_PATH, 'w', encoding='utf-8') as f:
    f.write(build_markdown_content(public_apis, is_private_doc=False))

print(f"[*] Private API 레퍼런스 파일 쓰는 중... ({PRIVATE_API_PATH})")
with open(PRIVATE_API_PATH, 'w', encoding='utf-8') as f:
    f.write(build_markdown_content(private_apis, is_private_doc=True))

print("[*] 빗썸 전체 API 레퍼런스 빌드가 정상적으로 종료되었습니다.")
