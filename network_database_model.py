"""
네트워크 데이터베이스 모델 (Network Database Model)
====================================================

Codd의 1970년 논문 "A Relational Model of Data for Large Shared Data Banks"에서
지적한 네트워크 모델의 문제점을 보여주는 예제입니다.

네트워크 모델의 특징:
- CODASYL(Conference on Data Systems Languages)에서 표준화
- 레코드들이 포인터(링크)로 연결된 그래프 구조
- 계층 모델과 달리 다대다 관계 표현 가능
- 하지만 여전히 포인터 기반 탐색 필요

Codd가 지적한 문제점:
1. 데이터 독립성 위반: 포인터 구조가 애플리케이션에 노출됨
2. 접근 경로 의존성: 데이터에 접근하려면 링크를 따라가야 함
3. 복잡한 스키마: Set 타입 정의와 관리의 복잡성
4. 프로그래머 부담: 포인터 조작 코드 필요
"""

from typing import Optional, List, Dict, Any, Set as PySet
from dataclasses import dataclass, field


@dataclass
class Record:
    """네트워크 DB의 레코드 (노드)"""
    record_type: str
    data: Dict[str, Any]
    record_id: str

    # 포인터들: 다른 레코드들과의 연결
    owner_links: Dict[str, 'Record'] = field(default_factory=dict)  # 소유자 포인터
    member_links: Dict[str, List['Record']] = field(default_factory=dict)  # 멤버 포인터

    def __hash__(self):
        return hash(self.record_id)


class NetworkSet:
    """
    CODASYL Set 구조

    네트워크 모델의 핵심: Owner-Member 관계
    - Owner: 부모 역할 (1)
    - Members: 자식들 역할 (N)
    - 하나의 레코드가 여러 Set의 Owner나 Member가 될 수 있음
    """

    def __init__(self, set_name: str, owner_type: str, member_type: str):
        self.set_name = set_name
        self.owner_type = owner_type
        self.member_type = member_type
        self.instances: List[Dict] = []  # [{owner: Record, members: [Record]}]

    def connect(self, owner: Record, member: Record):
        """Owner와 Member 연결 (포인터 설정)"""
        # 기존 인스턴스 찾기 또는 새로 생성
        instance = None
        for inst in self.instances:
            if inst['owner'] == owner:
                instance = inst
                break

        if instance is None:
            instance = {'owner': owner, 'members': []}
            self.instances.append(instance)

        instance['members'].append(member)

        # 양방향 포인터 설정
        if self.set_name not in owner.member_links:
            owner.member_links[self.set_name] = []
        owner.member_links[self.set_name].append(member)

        member.owner_links[self.set_name] = owner


class NetworkDatabase:
    """
    네트워크 데이터베이스 시뮬레이션

    CODASYL 스타일의 네트워크 DB 구현
    DML 명령어: FIND, GET, CONNECT, DISCONNECT 등 시뮬레이션
    """

    def __init__(self):
        self.records: Dict[str, Record] = {}
        self.sets: Dict[str, NetworkSet] = {}
        self.current_record: Optional[Record] = None  # Currency indicator

    def define_set(self, set_name: str, owner_type: str, member_type: str):
        """스키마 정의: Set 타입 생성"""
        self.sets[set_name] = NetworkSet(set_name, owner_type, member_type)

    def store_record(self, record: Record):
        """레코드 저장"""
        self.records[record.record_id] = record
        return record

    def find_record(self, record_id: str) -> Optional[Record]:
        """
        FIND 명령어 시뮬레이션

        네트워크 DB에서는 'currency indicator'를 설정함
        현재 작업 중인 레코드를 추적
        """
        if record_id in self.records:
            self.current_record = self.records[record_id]
            return self.current_record
        return None

    def find_owner_in_set(self, set_name: str) -> Optional[Record]:
        """
        현재 레코드의 Set에서 Owner 찾기

        *** 포인터 탐색 의존성 ***
        Owner를 찾으려면 반드시 포인터를 따라가야 함
        """
        if self.current_record and set_name in self.current_record.owner_links:
            owner = self.current_record.owner_links[set_name]
            self.current_record = owner
            return owner
        return None

    def find_members_in_set(self, set_name: str) -> List[Record]:
        """
        현재 레코드의 Set에서 Members 찾기

        *** 포인터 체인 탐색 ***
        """
        if self.current_record and set_name in self.current_record.member_links:
            return self.current_record.member_links[set_name]
        return []

    def connect_records(self, set_name: str, owner: Record, member: Record):
        """CONNECT 명령어: 레코드들을 Set에 연결"""
        if set_name in self.sets:
            self.sets[set_name].connect(owner, member)

    def build_sample_database(self):
        """
        샘플 데이터베이스 구축

        구조:
        - Departments (부서)
        - Employees (직원)
        - Projects (프로젝트)

        Sets:
        - DEPT_EMP: Department owns Employees
        - EMP_PROJ: Employee owns Projects
        - DEPT_PROJ: Department owns Projects (다른 경로)
        """
        # Set 타입 정의 (스키마)
        self.define_set("DEPT_EMP", "Department", "Employee")
        self.define_set("EMP_PROJ", "Employee", "Project")
        self.define_set("DEPT_PROJ", "Department", "Project")

        # 레코드 생성
        eng_dept = self.store_record(Record(
            "Department", {"name": "Engineering", "budget": 500000}, "D001"
        ))
        mkt_dept = self.store_record(Record(
            "Department", {"name": "Marketing", "budget": 300000}, "D002"
        ))

        alice = self.store_record(Record(
            "Employee", {"name": "Alice", "role": "Engineer"}, "E001"
        ))
        bob = self.store_record(Record(
            "Employee", {"name": "Bob", "role": "Engineer"}, "E002"
        ))
        charlie = self.store_record(Record(
            "Employee", {"name": "Charlie", "role": "Marketer"}, "E003"
        ))

        proj_alpha = self.store_record(Record(
            "Project", {"name": "Project Alpha", "status": "active"}, "P001"
        ))
        proj_beta = self.store_record(Record(
            "Project", {"name": "Project Beta", "status": "planning"}, "P002"
        ))
        campaign_x = self.store_record(Record(
            "Project", {"name": "Campaign X", "status": "active"}, "P003"
        ))

        # Set 연결 (포인터 설정)
        # DEPT_EMP: 부서 → 직원
        self.connect_records("DEPT_EMP", eng_dept, alice)
        self.connect_records("DEPT_EMP", eng_dept, bob)
        self.connect_records("DEPT_EMP", mkt_dept, charlie)

        # EMP_PROJ: 직원 → 프로젝트
        self.connect_records("EMP_PROJ", alice, proj_alpha)
        self.connect_records("EMP_PROJ", bob, proj_beta)
        self.connect_records("EMP_PROJ", charlie, campaign_x)

        # DEPT_PROJ: 부서 → 프로젝트 (부서가 직접 프로젝트 소유)
        self.connect_records("DEPT_PROJ", eng_dept, proj_alpha)
        self.connect_records("DEPT_PROJ", eng_dept, proj_beta)
        self.connect_records("DEPT_PROJ", mkt_dept, campaign_x)

        return self

    def print_structure(self):
        """데이터베이스 구조 출력"""
        print("\n[레코드 목록]")
        for rid, record in self.records.items():
            print(f"  {rid}: {record.record_type} - {record.data}")

        print("\n[Set 연결 구조]")
        for set_name, net_set in self.sets.items():
            print(f"\n  Set '{set_name}' ({net_set.owner_type} -> {net_set.member_type}):")
            for inst in net_set.instances:
                owner = inst['owner']
                members = inst['members']
                print(f"    Owner: {owner.data.get('name', owner.record_id)}")
                for m in members:
                    print(f"      └── Member: {m.data.get('name', m.record_id)}")


def demonstrate_pointer_dependence():
    """
    *** 포인터 의존성 문제 시연 ***

    네트워크 모델에서 쿼리 실행의 복잡성
    """
    print("=" * 70)
    print("포인터 의존성 문제 시연")
    print("=" * 70)

    db = NetworkDatabase().build_sample_database()
    db.print_structure()

    print("\n" + "-" * 70)
    print("[쿼리] Bob의 프로젝트들 찾기")
    print("-" * 70)
    print("""
네트워크 DB에서의 접근 방식 (CODASYL DML):

1. FIND RECORD Bob
2. SET CURRENT TO Bob
3. FIND FIRST MEMBER IN EMP_PROJ SET
4. GET PROJECT DATA
5. FIND NEXT MEMBER IN EMP_PROJ SET (반복)

코드:
""")

    # Bob 찾기
    bob = db.find_record("E002")
    if bob:
        print(f"  1. Bob 레코드 찾음: {bob.data}")

        # Bob의 프로젝트 찾기 (포인터 따라가기)
        projects = db.find_members_in_set("EMP_PROJ")
        print(f"  2. EMP_PROJ Set에서 멤버 탐색")
        for proj in projects:
            print(f"     → 프로젝트: {proj.data}")

    print("""
[문제점]
- 프로그래머가 포인터 구조를 알아야 함
- 'currency indicator' 관리 필요
- Set 이름을 정확히 알아야 함
- 실수로 잘못된 Set을 탐색하면 오류
""")


def demonstrate_access_path_complexity():
    """
    *** 접근 경로 복잡성 시연 ***

    같은 데이터에 여러 경로로 접근 가능하지만,
    각 경로마다 다른 코드가 필요
    """
    print("\n" + "=" * 70)
    print("접근 경로 복잡성 시연")
    print("=" * 70)

    db = NetworkDatabase().build_sample_database()

    print("""
[시나리오] "Project Alpha를 담당하는 부서 찾기"

경로 1: Project → (EMP_PROJ) → Employee → (DEPT_EMP) → Department
경로 2: Project → (DEPT_PROJ) → Department (직접)

두 경로 모두 유효하지만, 코드가 다릅니다!
""")

    print("-" * 70)
    print("경로 1: Project → Employee → Department")
    print("-" * 70)

    # 경로 1
    proj = db.find_record("P001")
    if proj:
        print(f"  1. Project Alpha 찾음")
        emp = db.find_owner_in_set("EMP_PROJ")
        if emp:
            print(f"  2. EMP_PROJ Owner: {emp.data['name']}")
            dept = db.find_owner_in_set("DEPT_EMP")
            if dept:
                print(f"  3. DEPT_EMP Owner: {dept.data['name']}")

    print("\n" + "-" * 70)
    print("경로 2: Project → Department (직접)")
    print("-" * 70)

    # 경로 2
    proj = db.find_record("P001")
    if proj:
        print(f"  1. Project Alpha 찾음")
        dept = db.find_owner_in_set("DEPT_PROJ")
        if dept:
            print(f"  2. DEPT_PROJ Owner: {dept.data['name']}")

    print("""
[Codd의 비판]
- 어떤 경로를 선택할지 프로그래머가 결정해야 함
- 스키마 변경 시 (Set 추가/삭제) 모든 관련 코드 수정 필요
- 최적의 경로는 데이터 분포에 따라 달라짐
- 이러한 결정이 애플리케이션 레벨에 있으면 안 됨!
""")


def demonstrate_schema_complexity():
    """
    *** 스키마 복잡성 시연 ***

    네트워크 모델의 Set 정의 복잡성
    """
    print("\n" + "=" * 70)
    print("스키마 복잡성 시연")
    print("=" * 70)

    print("""
[CODASYL 스키마 정의 예시]

SCHEMA NAME IS COMPANY-DB.

RECORD NAME IS DEPARTMENT.
    02 DEPT-ID       PIC X(4).
    02 DEPT-NAME     PIC X(30).
    02 BUDGET        PIC 9(10)V99.

RECORD NAME IS EMPLOYEE.
    02 EMP-ID        PIC X(4).
    02 EMP-NAME      PIC X(30).
    02 HIRE-DATE     PIC 9(8).

RECORD NAME IS PROJECT.
    02 PROJ-ID       PIC X(4).
    02 PROJ-NAME     PIC X(30).
    02 STATUS        PIC X(10).

SET NAME IS DEPT-EMP.
    OWNER IS DEPARTMENT.
    ORDER IS SORTED BY DEFINED KEYS.
    MEMBER IS EMPLOYEE
        INSERTION IS AUTOMATIC
        RETENTION IS MANDATORY
        KEY IS EMP-NAME ASCENDING.

SET NAME IS EMP-PROJ.
    OWNER IS EMPLOYEE.
    ORDER IS LAST.
    MEMBER IS PROJECT
        INSERTION IS MANUAL
        RETENTION IS OPTIONAL.

[문제점]
1. 복잡한 DDL 문법
2. INSERTION/RETENTION 옵션 관리
3. 새로운 관계 추가 시 Set 재정의 필요
4. 기존 프로그램과의 호환성 문제
""")


def demonstrate_data_independence_violation():
    """
    *** 데이터 독립성 위반 시연 ***
    """
    print("\n" + "=" * 70)
    print("데이터 독립성 위반")
    print("=" * 70)

    print("""
[시나리오] 새로운 요구사항 발생

"프로젝트가 여러 직원에게 할당될 수 있어야 함"
(1:N 관계를 M:N 관계로 변경)

[네트워크 모델에서의 해결]
1. 새로운 레코드 타입 생성: ASSIGNMENT
2. 기존 EMP_PROJ Set 수정 또는 삭제
3. 새로운 Set 생성:
   - EMP_ASSIGN: Employee owns Assignments
   - PROJ_ASSIGN: Project owns Assignments

[영향 범위]
- 스키마 전면 수정
- 모든 기존 프로그램의 DML 코드 수정
- 데이터 마이그레이션 필요
- 테스트 전면 재실행

[관계형 모델에서의 해결]
단순히 테이블 하나 추가:

CREATE TABLE Assignments (
    emp_id VARCHAR(4),
    proj_id VARCHAR(4),
    role VARCHAR(20),
    PRIMARY KEY (emp_id, proj_id),
    FOREIGN KEY (emp_id) REFERENCES Employees,
    FOREIGN KEY (proj_id) REFERENCES Projects
);

기존 쿼리는 그대로 동작!
새로운 관계만 새 테이블로 쿼리하면 됨.
""")


def compare_dml_operations():
    """
    DML 작업 비교: 네트워크 vs 관계형
    """
    print("\n" + "=" * 70)
    print("DML 작업 비교")
    print("=" * 70)

    print("""
[작업] Engineering 부서의 모든 직원과 그들의 프로젝트 조회

=== 네트워크 모델 (CODASYL DML) ===

MOVE 'Engineering' TO DEPT-NAME.
FIND ANY DEPARTMENT USING DEPT-NAME.
IF DB-STATUS = 0
    PERFORM UNTIL NO-MORE-EMPLOYEES
        FIND NEXT EMPLOYEE WITHIN DEPT-EMP
        IF DB-STATUS = 0
            GET EMPLOYEE
            DISPLAY EMP-NAME
            PERFORM UNTIL NO-MORE-PROJECTS
                FIND NEXT PROJECT WITHIN EMP-PROJ
                IF DB-STATUS = 0
                    GET PROJECT
                    DISPLAY PROJ-NAME
                END-IF
            END-PERFORM
        END-IF
    END-PERFORM
END-IF.

=== 관계형 모델 (SQL) ===

SELECT e.emp_name, p.proj_name
FROM Departments d
JOIN Employees e ON d.dept_id = e.dept_id
JOIN Projects p ON e.emp_id = p.emp_id
WHERE d.dept_name = 'Engineering';

[비교]
- 네트워크: 20+ 줄의 절차적 코드, 포인터 탐색, 상태 관리
- 관계형: 5줄의 선언적 코드, 무엇을 원하는지만 기술

Codd의 핵심 통찰:
"데이터 접근 방식이 아닌, 원하는 데이터가 무엇인지를 기술해야 한다"
""")


if __name__ == "__main__":
    print("=" * 70)
    print("네트워크 데이터베이스 모델의 한계")
    print("Codd (1970) 논문 기반 분석")
    print("=" * 70)

    demonstrate_pointer_dependence()
    demonstrate_access_path_complexity()
    demonstrate_schema_complexity()
    demonstrate_data_independence_violation()
    compare_dml_operations()

    print("\n" + "=" * 70)
    print("결론")
    print("=" * 70)
    print("""
Codd는 네트워크 모델의 다음 문제점들을 지적했습니다:

1. 포인터 의존성 (Pointer Dependence)
   - 프로그래머가 물리적 포인터 구조를 알아야 함
   - Currency indicator 관리의 복잡성

2. 접근 경로 의존성 (Access Path Dependence)
   - 여러 경로 중 어떤 것을 사용할지 결정 필요
   - 경로 선택이 성능에 영향

3. 스키마 변경의 어려움
   - Set 구조 변경 시 대규모 코드 수정 필요
   - 데이터 독립성 부재

4. 절차적 프로그래밍 강제
   - 선언적 쿼리 불가능
   - 레코드 단위 처리 (record-at-a-time)

관계형 모델의 해결책:
- 집합 기반 연산 (set-at-a-time)
- 선언적 쿼리 언어 (SQL)
- 논리적/물리적 데이터 독립성
- 수학적 기반 (관계 대수, 관계 해석)
""")
