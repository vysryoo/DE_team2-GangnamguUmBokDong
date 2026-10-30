# .agent

이 저장소에서 AI 에이전트가 따를 작업 규칙을 모아둔 디렉터리다.

> 이 README는 사람이 읽는 세팅 안내다. 에이전트는 이 파일을 읽을 필요가 없다. 규칙은 `AGENTS.md`와 이 디렉터리의 나머지 파일에 있다.

---

## 구성

```
프로젝트루트/
├── AGENTS.md                        # 공통 원칙 + 작업별 규칙 파일 안내
├── CLAUDE.md                        # @AGENTS.md 한 줄
├── .agent/
│   ├── README.md                    # 이 파일
│   ├── coding.md                    # 코드 구현 규칙
│   ├── commit.md                    # 커밋 메시지 규칙
│   └── pr.md                        # PR 작성 규칙
├── .claude/settings.json            # AI 서명 차단
└── .github/pull_request_template.md # PR 본문 틀
```

| 파일 | 역할 | 로드 시점 |
| --- | --- | --- |
| `AGENTS.md` | 공통 원칙과 라우팅 | 항상 |
| `CLAUDE.md` | Claude Code용 브리지 | 항상 |
| `.agent/coding.md` | 네이밍, 주석, docstring | 코드 수정 전 |
| `.agent/commit.md` | 제목 형식, 커밋 분할 | 커밋 직전 |
| `.agent/pr.md` | 제목, 본문 템플릿 | PR 생성 직전 |

### 왜 나눠져 있는가

`AGENTS.md`는 매 세션 컨텍스트에 통째로 들어간다. 규칙을 전부 한 파일에 넣으면 코드 한 줄 고치는 세션에서도 PR 템플릿과 커밋 분할 절차가 함께 로드되어 컨텍스트를 낭비하고, 문서가 길수록 개별 지시의 준수율이 떨어진다.

그래서 항상 필요한 공통 원칙만 `AGENTS.md`에 두고, 특정 작업에서만 쓰이는 규칙은 이 디렉터리로 분리했다. `AGENTS.md`는 어떤 상황에 어떤 파일을 읽어야 하는지만 알려준다.

`CLAUDE.md`가 따로 있는 이유는 Claude Code가 `AGENTS.md`를 읽지 않기 때문이다. `@AGENTS.md` 한 줄로 import해 두 도구가 같은 규칙을 보게 한다.

---

## 신규 세팅

### 1. 파일 배치

위 구조대로 파일을 저장소 루트에 복사한다.

### 2. `.gitignore` 확인

`.agent/`는 점으로 시작하므로, `.gitignore`에 `.*` 같은 광범위한 패턴이 있으면 커밋에서 누락된다.

```bash
git check-ignore -v .agent/coding.md
```

출력이 없어야 정상이다. 무언가 출력되면 `.gitignore`에 `!.agent/`를 추가한다.

### 3. 커밋

```bash
git add AGENTS.md CLAUDE.md .agent/ .claude/settings.json .github/
git commit -m "chore: AI 에이전트 작업 규칙 추가"
```

### 4. 로드 확인

Claude Code 세션에서 `/context`를 실행해 **Memory files** 목록에 `CLAUDE.md`가 있는지 확인한다. 목록에 없으면 규칙이 전혀 적용되지 않고 있는 것이다.

### 5. 동작 확인

작은 변경 하나로 커밋과 PR을 만들어보고 다음을 확인한다.

- 커밋 제목이 `<type>: <요약>` 형식이고 70자 이내인가
- 커밋에 `Co-Authored-By` 트레일러가 붙지 않는가
- PR 본문이 3개 섹션 구조를 따르는가
- PR 본문 하단에 생성 문구가 없는가

---

## 다른 도구를 함께 쓸 때

| 도구 | 읽는 위치 |
| --- | --- |
| Claude Code | `CLAUDE.md` |
| Cursor | `.cursor/rules/` |
| GitHub Copilot | `.github/copilot-instructions.md` |

각 위치에 `AGENTS.md`를 복사하지 않는다. 참조 한 줄만 둔다. 원본이 여러 벌로 갈라지면 곧 서로 어긋난다.

---

## 규칙 수정 시 주의

**중복된 항목이 있다.** 다음 세 가지는 `AGENTS.md`의 「작업 범위」와 `.agent/coding.md` 1번에 모두 있다. `coding.md`를 읽지 않고 코드를 건드리는 상황에 대비한 의도적 중복이므로, 수정할 때는 두 파일을 함께 본다.

- 기존 코드 관례 우선
- 요청받지 않은 파일 생성 금지
- 무관한 라인 포매팅 금지

**type 목록은 `AGENTS.md` 부록에만 있다.** `commit.md`와 `pr.md`가 이를 참조한다. 파일을 다른 저장소로 옮길 때 `AGENTS.md` 없이 `.agent/`만 복사하면 type 정보가 사라진다.

**AI 서명 금지는 두 겹으로 걸려 있다.** `.claude/settings.json`의 `attribution`은 CLI가 자동으로 덧붙이는 부분을 막고, `AGENTS.md`의 규칙은 모델이 본문에 직접 써넣는 것을 막는다. 둘 중 하나만 있으면 새어 나간다. 또한 팀원이 각자의 `.claude/settings.local.json`으로 설정을 덮어쓸 수 있으므로, 반드시 차단해야 한다면 커밋 훅을 함께 건다.

**규칙은 추가보다 삭제가 어렵다.** 문제가 생길 때마다 조항을 붙이면 아무도 읽지 않는 문서가 된다. 새 규칙을 넣을 때 뺄 항목이 없는지 함께 본다. 특히 린터나 포매터로 강제할 수 있는 항목은 문서가 아니라 설정으로 옮긴다.
