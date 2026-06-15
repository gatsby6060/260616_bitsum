# 빗썸 API JWT 인증 및 query_hash 해싱 규칙

빗썸 Private API는 호출 시 헤더에 HMAC-SHA256 알고리즘으로 서명된 JWT(JSON Web Token)를 필요로 합니다.
빗썸의 JWT 검증 로직은 일반적인 JWT와 달리, **요청 파라미터를 쿼리 스트링으로 만들어 SHA-512로 해싱한 결과(`query_hash`)**를 페이로드에 반드시 포함해야 합니다.

이 문서는 AI 모델과 개발자가 인증 관련 버그 없이 코드를 완벽하게 구현할 수 있도록 상세 규칙과 구현 예제를 제공합니다.

---

## 1. JWT Payload 구조

### A. 파라미터가 없는 요청 (예: GET /v1/accounts)
파라미터가 없거나 바디가 비어 있는 요청은 기본 3개 필드만 사용합니다.

```json
{
  "access_key": "YOUR_API_ACCESS_KEY",
  "nonce": "UUID_STRING_OR_UNIQUE_STRING",
  "timestamp": 1712230310689
}
```

### B. 파라미터가 있는 요청 (예: GET /v1/orders/chance?market=KRW-BTC)
요청 파라미터(Query String 또는 JSON Body)가 존재하면 아래 5개 필드를 모두 채워야 합니다.

```json
{
  "access_key": "YOUR_API_ACCESS_KEY",
  "nonce": "UUID_STRING_OR_UNIQUE_STRING",
  "timestamp": 1712230310689,
  "query_hash": "SHA512_HEX_STRING_OF_QUERY_STRING",
  "query_hash_alg": "SHA512"
}
```

| 페이로드 필드 | 타입 | 필수 여부 | 설명 |
| :--- | :--- | :--- | :--- |
| `access_key` | string | **필수** | 빗썸에서 발급받은 API Access Key |
| `nonce` | string | **필수** | 매 요청마다 고유해야 하는 문자열. UUIDv4 사용을 적극 권장합니다. |
| `timestamp` | integer | **필수** | 요청 시점의 Unix Millisecond Timestamp (밀리초 단위의 타임스탬프) |
| `query_hash` | string | **조건부 필수** | 요청 파라미터들을 직렬화한 쿼리 스트링을 SHA-512로 해싱한 16진수 문자열 |
| `query_hash_alg` | string | **조건부 필수** | 해시 알고리즘 정보. 반드시 `"SHA512"`로 고정합니다. |

---

## 2. query_hash 해싱 상세 규칙

빗썸의 `query_hash` 검증 오류(`invalid_query_hash`)를 막으려면 아래의 3가지 규칙을 완벽하게 준수해야 합니다.

### 규칙 1: 모든 HTTP 메서드 파라미터의 단일 포맷화
* **GET / DELETE**: URL 뒤에 붙는 쿼리 파라미터들(`?market=KRW-BTC&state=wait`)을 해싱합니다.
* **POST / PUT**: Request Body에 JSON으로 들어갈 데이터(`{"market": "KRW-BTC", "side": "bid", "price": "100"}`)를 **쿼리 스트링 형태**(`market=KRW-BTC&side=bid&price=100`)로 변환한 후 해싱합니다.

### 규칙 2: 쿼리 파라미터 정렬 및 키-값 인코딩
* 파라미터들은 키(Key) 기준으로 **알파벳 사전순(Ascending order)**으로 정렬하여 직렬화하는 것이 안전합니다.
* 특수문자나 한글이 포함된 파라미터 값은 반드시 `URL Encoding`을 거쳐야 합니다. (예: `market=KRW-BTC` -> `market=KRW-BTC`)

### 규칙 3: 배열 파라미터 직렬화 방식 (중요)
* 다건 조회 등을 할 때 배열 파라미터가 포함되는 경우(예: `uuids = ['id1', 'id2']`), 빗썸은 대괄호 표기법(`[]`)을 명시하여 직렬화해야 합니다.
  * 올바른 직렬화 형태: `uuids[]=id1&uuids[]=id2`
  * 인코딩 결과: `uuids%5B%5D=id1&uuids%5B%5D=id2` (Python `urlencode` 시 `doseq=True` 옵션 지정 필요)

---

## 3. 언어별 구현 코드 예제

### Python 3 예제

Python에서는 `jwt` 패키지(PyJWT)와 표준 라이브러리인 `hashlib`, `urllib.parse`를 활용하여 아래와 같이 구현합니다.

```python
import hashlib
import time
import uuid
import jwt
import requests
from urllib.parse import urlencode

def create_bithumb_headers(access_key: str, secret_key: str, params: dict = None) -> dict:
    payload = {
        "access_key": access_key,
        "nonce": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000)
    }

    if params:
        # 1. 딕셔너리를 쿼리 스트링으로 인코딩 (배열 처리를 위해 doseq=True 설정)
        # 빗썸 검증 서버와 일치시키기 위해 정렬하여 인코딩하는 것이 좋습니다.
        sorted_params = sorted(params.items())
        query_string = urlencode(sorted_params, doseq=True).encode("utf-8")
        
        # 2. SHA-512 해싱 진행
        query_hash = hashlib.sha512(query_string).hexdigest()
        payload["query_hash"] = query_hash
        payload["query_hash_alg"] = "SHA512"

    # 3. HS256 알고리즘으로 서명 및 토큰 발행
    jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
    
    return {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }

# 사용 예시 (POST 주문 요청)
# api_key = "..."
# secret = "..."
# order_params = {
#     "market": "KRW-BTC",
#     "side": "bid",
#     "volume": "0.001",
#     "price": "85000000",
#     "ord_type": "limit"
# }
# headers = create_bithumb_headers(api_key, secret, order_params)
# response = requests.post("https://api.bithumb.com/v2/orders", json=order_params, headers=headers)
# print(response.json())
```

### Node.js (JavaScript) 예제

Node.js 환경에서는 `jsonwebtoken` 라이브러리와 내장 `crypto` 모듈을 사용합니다. 쿼리 스트링 변환에는 `qs` 라이브러리를 사용하면 배열 포맷 직렬화(`arrayFormat: 'brackets'`)가 기본 지원되므로 편리합니다.

```javascript
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const crypto = require('crypto');
const qs = require('qs');

function createBithumbHeaders(accessKey, secretKey, params = null) {
    const payload = {
        access_key: accessKey,
        nonce: uuidv4(),
        timestamp: Date.now()
    };

    if (params) {
        // qs.stringify를 사용하여 객체를 'uuids[]=id1&uuids[]=id2' 형태로 직렬화합니다.
        // 또한 빗썸 서버의 일관성을 위해 키 정렬(sort)을 수행합니다.
        const queryString = qs.stringify(params, { 
            arrayFormat: 'brackets',
            sort: (a, b) => a.localeCompare(b)
        });

        // SHA-512 해싱 수행
        const queryHash = crypto
            .createHash('sha512')
            .update(queryString, 'utf-8')
            .digest('hex');

        payload.query_hash = queryHash;
        payload.query_hash_alg = 'SHA512';
    }

    // HS256 서명 진행
    const jwtToken = jwt.sign(payload, secretKey, { algorithm: 'HS256' });

    return {
        'Authorization': `Bearer ${jwtToken}`,
        'Content-Type': 'application/json'
    };
}
```

---

## 4. 디버깅 및 에러 해결 가이드

### A. `invalid_query_hash` 에러 발생 시 체크리스트
1. **타입 불일치**: `requests.post(url, json=params)` 처럼 요청 바디에 실어 보내는 데이터 구조와, 헤더를 생성할 때 `params`로 넘긴 데이터 구조(키/값)가 문자 수준까지 완벽하게 동일합니까?
2. **숫자형 vs 문자형**: JSON Body로 보낼 때 가격이나 수량을 `"1000"`처럼 문자열로 보냈다면, 해싱 쿼리를 만들 때도 문자열 `"1000"`이어야 합니다. `1000` (Number)과 `"1000"` (String)은 쿼리 스트링 변환 시 일부 언어에서 다르게 변환되거나 유휴 처리에 오류를 유발할 수 있으므로, **모든 수치 데이터를 처음부터 문자열(`str`) 형식으로 다루는 것이 권장됩니다.**
3. **URL 인코딩 공백 처리**: 공백 문자를 `%20` 대신 `+`로 인코딩하여 해시를 만들었는지 확인하십시오. 빗썸의 해시 검증기는 `%20` 표준을 기대합니다. (Python의 `urlencode`는 기본적으로 공백을 `+`로 변환하므로, `quote_via` 인자로 `urllib.parse.quote`를 지정하는 것이 안전합니다.)

### B. `jwt_verification_failed` 에러 발생 시 체크리스트
1. API Key 혹은 Secret Key가 정확히 복사되었는지 확인하십시오. (특히 앞뒤 공백 문자 유입 주의)
2. 로컬 시스템 시각이 표준 시각과 30초 이상 벌어져 있는지 확인하십시오. 빗썸 서버 시각과 단말 시각차가 클 경우 JWT 검증이 실패합니다. 가능하면 동기화 도구(NTP)를 켜두어야 합니다.
