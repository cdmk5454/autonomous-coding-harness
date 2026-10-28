# v1 → v2 공개 및 운영 전환 체크리스트

`[x]`는 해당 범위의 근거가 확인되었다는 뜻입니다. 정적 검사 PASS를 실제 Provider 실행·운영 검증으로 확대하지 않습니다.

## 1. 기존 이력 보존

- [x] 이전 `main` 기준점: `4e9393fcf56ca23f5da177dcfd63719a135f5523` (공개 0.99.2.1)
- [x] `archive/0.99.2.1-python-kernel` 브랜치로 이전 공개 코드 보존
- [ ] 별도 실험용 Git 아카이브의 민감한 경로·도메인 식별자 제거 및 공개 적합성 검증
- [x] 0.99.2.1을 0.99.1 또는 검증된 1.0/1.1 릴리스로 혼동하지 않도록 구분

## 2. 공개 자료 및 Git 안전성

- [x] 실제 `profiles/`, `config.json`, 비밀 키·로그·`.control`/`.tasks`·중첩 ZIP 제외
- [x] 출처 및 라이선스 검토 전 외부 Third-party Skill은 공개 패키지에서 제외
- [x] 승인한 ZIP과 v2 Git Tree의 파일 목록·내용 일치 검증
- [x] 이전 `main`과 v2 후보의 변경·삭제 경로 비교 확인
- [ ] GitHub Code/Secret Scanning 검사 결과 확인 (접근·실행 여부에 따라 별도 관리)
- [ ] GitHub Actions / 자동화 검사 실행 및 결과 확인 (현재 v2 후보에 CI가 구성되지 않은 경우 `NOT_RUN`)
- [ ] 최종 공개 브랜치 이동·병합 SHA 및 실제 Git Tree 사후 대조

## 3. 실제 운영 검증 — 공개 후보에서 미완료

- [ ] Provider·모드별 Tool denial과 실제 Write 방지 확인
- [ ] Shell·Subprocess를 포함한 실질 Read-only 경계 검증
- [ ] 허용 Workspace·경로, Diff ownership, 후보 격리, rollback/recovery 확인
- [ ] Runtime 실패와 Product 실패 분류 및 bounded retry 동작 확인
- [ ] Build/Test/Review/QA Evidence가 최종 변경과 동일 입력에 귀속되는지 확인
- [ ] Cross-family Reviewer가 구현 후보를 변경하지 못하는지 확인
- [ ] Credential redaction 및 프로젝트별 DB/API/Data Contract 승인 절차 확인
- [ ] 실제 LOW Job, 실패 복구, Human QA 격리, 통합 검증 및 Batch publication 시나리오 수행

**판정:** 현재 공개할 수 있는 것은 `2.0.0-candidate`의 문서·정책·샘플 구조입니다. 위 운영 항목을 수행하기 전에는 `V2_OPERATIONAL_PASS`나 동등한 상태를 선언하지 않습니다.
