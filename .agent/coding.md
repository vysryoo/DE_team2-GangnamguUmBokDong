# 코드 구현 규칙

적용 언어: Python, Java / Kotlin, TypeScript

`AGENTS.md`의 공통 원칙이 이 문서보다 우선한다.

---

## 1. 최우선 원칙

**기존 코드의 관례가 이 문서보다 우선한다.** 파일을 수정할 때는 먼저 주변 코드를 읽고 그 파일에서 쓰이는 네이밍과 주석 방식을 따른다. 이 문서와 어긋나더라도 한 파일 안에서 스타일이 섞이는 것보다 낫다. 기존 관례를 바꿔야 한다고 판단되면 임의로 바꾸지 말고 먼저 알린다.

**포매팅은 포매터에 맡긴다.** 들여쓰기, 줄바꿈, 따옴표, 임포트 정렬 등은 프로젝트 설정을 따르며 직접 판단하지 않는다. 작업과 무관한 파일이나 라인을 포매팅 목적으로 건드리지 않는다. diff에 관계없는 변경이 섞이면 리뷰가 불가능해진다.

| 언어 | 도구 |
| --- | --- |
| Python | Ruff, Black |
| Java | Spotless, Checkstyle |
| Kotlin | ktlint |
| TypeScript | Prettier, ESLint |

프로젝트에 설정 파일이 있으면 그 설정이 절대 기준이다.

---

## 2. 네이밍 표기 규칙

각 언어의 표준 관례를 따른다. 언어를 가로질러 통일하지 않는다.

### Python (PEP 8)

| 대상 | 표기 |
| --- | --- |
| 변수, 함수, 메서드, 모듈 | `snake_case` |
| 클래스 | `PascalCase` |
| 상수 | `UPPER_SNAKE_CASE` |
| 내부용 | `_leading_underscore` |

### Java / Kotlin

| 대상 | 표기 |
| --- | --- |
| 변수, 함수, 메서드 | `camelCase` |
| 클래스, 인터페이스 | `PascalCase` |
| 상수 | `UPPER_SNAKE_CASE` |
| 패키지 | 전부 소문자, 구분자 없음 |

Kotlin에서 최상위 `val` 중 컴파일 타임 상수는 `UPPER_SNAKE_CASE`, 그 외 불변 프로퍼티는 `camelCase`를 쓴다.

### TypeScript

| 대상 | 표기 |
| --- | --- |
| 변수, 함수, 메서드 | `camelCase` |
| 클래스, 타입, 인터페이스, enum | `PascalCase` |
| 상수 | `UPPER_SNAKE_CASE` 또는 `camelCase` (프로젝트 관례를 따름) |
| React 컴포넌트 | `PascalCase` |

인터페이스에 `I` 접두사를 붙이지 않는다. 타입 이름에 `Type` 접미사를 기계적으로 붙이지 않는다.

---

## 3. 이름의 질

표기법보다 중요한 것은 이름이 실제로 무엇인지 알려주는가다.

### 의미 없는 이름 금지

`data`, `info`, `temp`, `result`, `value`, `item`, `obj`, `handle`, `process`, `manager`, `helper`, `util` 를 단독으로 쓰지 않는다. 무엇에 대한 데이터인지, 무엇의 결과인지를 이름에 담는다.

```
나쁨: const data = await fetchUser(id)
좋음: const user = await fetchUser(id)

나쁨: def process(items):
좋음: def filter_expired_orders(orders):

나쁨: class OrderHelper
좋음: class OrderPriceCalculator
```

예외로 관례가 굳어진 짧은 이름은 허용한다. 반복문의 `i`, 좌표의 `x`/`y`, 예외 변수 `e`, 람다의 짧은 인자 등이다.

### 과도하게 긴 이름 금지

이름에 구현 과정을 나열하지 않는다. 이름이 길어진다면 대개 함수가 두 가지 일을 하고 있다는 신호다.

```
나쁨: getUserDataFromDatabaseAndValidateThenCache()
좋음: findUser() / validateUser() / cacheUser()
```

컨텍스트로 알 수 있는 정보를 이름에 중복하지 않는다.

```
나쁨: class User { userName; userEmail; userId; }
좋음: class User { name; email; id; }
```

### 함수 이름은 동사로

무엇을 하는지가 드러나야 한다. 부수효과가 있는 함수와 없는 함수를 이름으로 구분한다.

- 조회: `find`, `get`, `fetch`
- 판별: `is`, `has`, `can` 접두사, 반환은 boolean
- 변환: `to`, `parse`, `format`
- 생성: `create`, `build`
- 상태 변경: `update`, `set`, `save`, `delete`

`get`으로 시작하는 함수가 DB에 쓰기를 하거나 외부 상태를 바꾸지 않도록 한다.

### 축약어

널리 쓰이는 것만 사용한다. `id`, `url`, `api`, `db`, `req`, `res`는 허용하고, `usr`, `calc`, `mng`, `btn2`처럼 임의로 줄인 이름은 쓰지 않는다.

### 불리언

긍정형으로 짓는다. `isNotValid` 대신 `isValid`를 쓴다. 부정형은 `if (!isNotValid)` 같은 이중 부정을 만든다.

---

## 4. 주석

**주석은 최소한으로 쓴다.** 기본값은 주석 없음이다. 좋은 이름과 작은 함수로 설명되는 것을 주석으로 대신하지 않는다.

### 쓰지 않는 주석

코드를 한국어로 옮긴 주석을 달지 않는다. 코드가 바뀌면 곧바로 거짓말이 되고, 읽는 사람의 시간만 뺏는다.

```
나쁨:
# 사용자를 조회한다
user = find_user(user_id)

나쁨:
// 리스트를 순회하며 합계를 구한다
for (const item of items) { total += item.price }
```

다음도 달지 않는다.

- 섹션 구분용 장식 주석 (`# ===== 유틸 함수 =====`)
- 변경 이력 (`# 2026-08-31 김철수 수정`) — git이 이미 기록한다
- 주석 처리된 코드 — 삭제한다. 필요하면 git에서 되살린다
- 작업 과정 서술 (`# 여기서 캐시를 추가했음`)

### 쓰는 주석

코드에 드러나지 않는 **이유**만 남긴다. 무엇을 하는지가 아니라 왜 그렇게 했는지다.

```
좋음:
# 결제 게이트웨이가 초당 10건으로 제한하므로 배치 크기를 맞춤
BATCH_SIZE = 10

좋음:
// 레거시 API가 빈 배열 대신 null을 반환함 (티켓 #482)
const items = response.items ?? []

좋음:
# 정렬 안정성이 필요해 sorted() 사용. sort()로 바꾸면 순서가 깨짐
```

주석을 달아야 하는 경우는 대체로 다음과 같다.

- 직관에 반하는 선택을 한 이유
- 외부 시스템의 버그나 제약을 우회하는 코드
- 성능 때문에 가독성을 포기한 부분
- 알고리즘의 출처나 참조 링크
- 임시 조치임을 알리는 `TODO` / `FIXME` (근거와 티켓 번호를 함께)

---

## 5. Docstring / JSDoc / KDoc

**공개 API에는 반드시 작성한다.** 다른 모듈이나 외부에서 호출하는 함수, 클래스, 공개 인터페이스가 대상이다. 내부 헬퍼 함수와 private 메서드에는 달지 않는다.

각 언어의 표준 구조를 빠짐없이 채운다. 모든 인자와 반환값을 항목으로 기술하며, 타입에서 유추된다는 이유로 생략하지 않는다. 문서 생성 도구와 IDE 툴팁이 이 구조를 읽어가므로 형식의 일관성이 개별 항목의 간결함보다 우선한다.

```python
def create_order(user_id: int, items: list[Item]) -> Order:
    """주문을 생성한다.

    Args:
        user_id: 사용자 ID
        items: 아이템 목록
    Returns:
        생성된 주문
    """
```

### 담는 내용

기본 구조를 채운 뒤, 시그니처에서 읽히지 않는 정보가 있으면 설명에 덧붙인다.

- 부수효과, 상태 변경, 외부 시스템 호출
- 발생 가능한 예외와 그 조건
- 인자의 제약 조건 (허용 범위, null 허용 여부, 단위)
- 사용 예시 (사용법이 자명하지 않을 때만)

```python
def create_order(user_id: int, items: list[Item]) -> Order:
    """재고를 선점한 뒤 주문을 생성한다.

    Args:
        user_id: 사용자 ID
        items: 주문할 아이템 목록. 비어 있으면 안 됨
    Returns:
        생성된 주문. 상태는 PENDING으로 시작함
    Raises:
        OutOfStockError: 재고가 부족한 경우. 주문은 생성되지 않음
    """
```

### 형식

| 언어 | 형식 | 필수 항목 |
| --- | --- | --- |
| Python | PEP 257 docstring. 프로젝트가 Google/NumPy 스타일을 쓰면 그에 맞춤 | `Args`, `Returns`, `Raises` |
| Java | Javadoc | `@param`, `@return`, `@throws` |
| Kotlin | KDoc | `@param`, `@return`, `@throws` |
| TypeScript | JSDoc | `@param`, `@returns`, `@throws` |

TypeScript는 타입이 시그니처에 이미 있으므로 JSDoc에 타입 표기(`{string}`)를 중복해서 넣지 않는다. 태그와 설명만 쓴다.

---

## 6. 문서 갱신

코드 변경이 기존 문서를 낡게 만들면 함께 고친다. 대상은 다음과 같다.

- 공개 API의 시그니처나 동작이 바뀐 경우
- 설정값, 환경변수, 실행 명령이 추가되거나 바뀐 경우
- README나 문서에 적힌 내용과 코드가 어긋나게 된 경우

반대로 요청받지 않은 문서를 새로 만들지 않는다. 변경 요약 문서, 작업 내역 파일, 설계 문서를 임의로 생성하지 않는다. 그런 내용은 커밋 메시지와 PR 본문에 들어간다.
