"""
계층적 데이터베이스 모델 (Hierarchical Database Model)
=======================================================

Codd의 1970년 논문 "A Relational Model of Data for Large Shared Data Banks"에서
지적한 계층적 모델의 문제점을 보여주는 예제입니다.

계층적 모델의 특징:
- 데이터가 트리 구조로 구성됨 (부모-자식 관계)
- 각 자식은 오직 하나의 부모만 가질 수 있음
- 데이터 접근은 루트에서 시작하여 경로를 따라 탐색해야 함

Codd가 지적한 문제점:
1. 데이터 독립성 위반: 물리적 저장 구조가 논리적 접근에 영향을 미침
2. 접근 경로 의존성: 특정 경로로만 데이터에 접근 가능
3. 중복 데이터 문제: 다대다 관계 표현을 위해 데이터 중복 필요
4. 일관성 유지 어려움: 중복된 데이터의 동기화 문제
"""

from typing import Optional, List, Dict, Any


class HierarchicalNode:
    """계층적 데이터베이스의 노드를 나타내는 클래스"""

    def __init__(self, data: Dict[str, Any], parent: Optional['HierarchicalNode'] = None):
        self.data = data
        self.parent = parent
        self.children: List['HierarchicalNode'] = []

        if parent:
            parent.children.append(self)

    def get_path(self) -> str:
        """루트에서 현재 노드까지의 경로 반환"""
        path = []
        node = self
        while node:
            path.append(str(node.data))
            node = node.parent
        return " -> ".join(reversed(path))


class HierarchicalDatabase:
    """
    계층적 데이터베이스 시뮬레이션

    예시: 회사 조직도와 프로젝트 관리

    문제점 시연:
    - 한 직원이 여러 부서에서 일하는 경우 → 데이터 중복 필요
    - 프로젝트를 찾으려면 부서 → 직원 → 프로젝트 경로로만 접근 가능
    """

    def __init__(self):
        self.root = None

    def build_sample_database(self):
        """
        회사 조직 구조 예시:

        Company (root)
        ├── Engineering Dept
        │   ├── Alice (Engineer)
        │   │   └── Project Alpha
        │   └── Bob (Engineer)
        │       └── Project Beta
        └── Marketing Dept
            └── Charlie (Marketer)
                └── Campaign X

        문제: Bob이 Marketing에서도 일한다면?
        → 계층 모델에서는 Bob 노드를 복제해야 함!
        """
        # 루트: 회사
        self.root = HierarchicalNode({"type": "company", "name": "TechCorp"})

        # Level 1: 부서들
        engineering = HierarchicalNode(
            {"type": "department", "name": "Engineering", "budget": 500000},
            parent=self.root
        )
        marketing = HierarchicalNode(
            {"type": "department", "name": "Marketing", "budget": 300000},
            parent=self.root
        )

        # Level 2: 직원들
        alice = HierarchicalNode(
            {"type": "employee", "id": "E001", "name": "Alice", "role": "Engineer"},
            parent=engineering
        )
        bob = HierarchicalNode(
            {"type": "employee", "id": "E002", "name": "Bob", "role": "Engineer"},
            parent=engineering
        )
        charlie = HierarchicalNode(
            {"type": "employee", "id": "E003", "name": "Charlie", "role": "Marketer"},
            parent=marketing
        )

        # Level 3: 프로젝트들
        HierarchicalNode(
            {"type": "project", "name": "Project Alpha", "status": "active"},
            parent=alice
        )
        HierarchicalNode(
            {"type": "project", "name": "Project Beta", "status": "planning"},
            parent=bob
        )
        HierarchicalNode(
            {"type": "project", "name": "Campaign X", "status": "active"},
            parent=charlie
        )

        return self

    def navigate_from_root(self, path: List[int]) -> Optional[HierarchicalNode]:
        """
        *** 접근 경로 의존성 문제 ***

        계층적 DB에서 데이터에 접근하려면 루트에서부터
        정확한 경로(인덱스)를 알아야 합니다.

        path: 각 레벨에서 선택할 자식의 인덱스 리스트
        예: [0, 1, 0] = 첫번째 부서 → 두번째 직원 → 첫번째 프로젝트
        """
        node = self.root
        for idx in path:
            if node and idx < len(node.children):
                node = node.children[idx]
            else:
                return None
        return node

    def find_by_attribute(self, attr: str, value: Any,
                          node: Optional[HierarchicalNode] = None) -> List[HierarchicalNode]:
        """
        *** 비효율적인 검색 ***

        계층적 DB에서 속성으로 검색하려면 전체 트리를 순회해야 합니다.
        이는 경로 기반 접근의 한계를 보여줍니다.
        """
        if node is None:
            node = self.root

        results = []
        if node.data.get(attr) == value:
            results.append(node)

        for child in node.children:
            results.extend(self.find_by_attribute(attr, value, child))

        return results

    def print_tree(self, node: Optional[HierarchicalNode] = None, level: int = 0):
        """트리 구조 출력"""
        if node is None:
            node = self.root

        indent = "    " * level
        prefix = "└── " if level > 0 else ""
        print(f"{indent}{prefix}{node.data}")

        for child in node.children:
            self.print_tree(child, level + 1)


def demonstrate_data_independence_violation():
    """
    *** 데이터 독립성 위반 시연 ***

    Codd의 핵심 비판: 물리적 저장 구조의 변경이
    애플리케이션 코드의 변경을 요구함
    """
    print("=" * 70)
    print("데이터 독립성 위반 시연")
    print("=" * 70)

    db = HierarchicalDatabase().build_sample_database()

    print("\n[현재 트리 구조]")
    db.print_tree()

    # 문제 1: 경로 기반 접근
    print("\n[문제 1: 경로 의존적 접근]")
    print("Bob의 프로젝트를 찾으려면 정확한 경로가 필요합니다:")
    print("경로: root → Engineering(0) → Bob(1) → Project Beta(0)")

    bob_project = db.navigate_from_root([0, 1, 0])
    if bob_project:
        print(f"찾은 프로젝트: {bob_project.data}")
        print(f"전체 경로: {bob_project.get_path()}")

    # 문제 2: 구조 변경의 영향
    print("\n[문제 2: 구조 변경 시 코드 수정 필요]")
    print("만약 부서 순서가 바뀌면 (Marketing이 첫번째로):")
    print("→ 기존 코드의 모든 경로 인덱스를 수정해야 함!")
    print("→ 이것이 '데이터 독립성 위반'입니다.")


def demonstrate_data_redundancy():
    """
    *** 데이터 중복 문제 시연 ***

    다대다 관계를 표현하려면 데이터를 복제해야 함
    → 일관성 유지가 어려워짐
    """
    print("\n" + "=" * 70)
    print("데이터 중복 문제 시연")
    print("=" * 70)

    print("""
[시나리오] Bob이 Engineering과 Marketing 두 부서에서 일한다면?

계층적 모델에서는 Bob을 복제해야 합니다:

Company
├── Engineering Dept
│   ├── Alice
│   │   └── Project Alpha
│   └── Bob (복사본 1) ← 같은 사람
│       └── Project Beta
└── Marketing Dept
    ├── Charlie
    │   └── Campaign X
    └── Bob (복사본 2) ← 같은 사람!
        └── Campaign Y

[문제점]
1. 저장 공간 낭비
2. Bob의 정보 변경 시 모든 복사본을 수정해야 함
3. 동기화 실패 시 데이터 불일치 발생 (일관성 위반)
4. "Bob은 몇 개의 프로젝트에 참여하나요?" 쿼리가 복잡해짐
""")


def demonstrate_query_limitations():
    """
    *** 쿼리 제한 시연 ***

    계층적 모델에서 복잡한 쿼리의 어려움
    """
    print("\n" + "=" * 70)
    print("쿼리 제한 시연")
    print("=" * 70)

    db = HierarchicalDatabase().build_sample_database()

    print("\n[쿼리 1: 모든 활성 프로젝트 찾기]")
    print("→ 전체 트리를 순회해야 함 (비효율적)")

    active_projects = db.find_by_attribute("status", "active")
    print(f"찾은 프로젝트 수: {len(active_projects)}")
    for proj in active_projects:
        print(f"  - {proj.data['name']} (경로: {proj.get_path()})")

    print("\n[쿼리 2: 'Project Beta'를 담당하는 직원 찾기]")
    print("→ 프로젝트에서 직원으로 역방향 탐색이 필요")

    projects = db.find_by_attribute("name", "Project Beta")
    if projects:
        # 부모로 올라가야 직원을 찾을 수 있음
        employee = projects[0].parent
        print(f"담당 직원: {employee.data['name']}")
        print("→ 역방향 포인터가 필요! (추가 저장 공간과 유지보수 비용)")


def compare_with_relational():
    """
    관계형 모델과의 비교
    """
    print("\n" + "=" * 70)
    print("관계형 모델과의 비교")
    print("=" * 70)

    print("""
[관계형 모델에서의 표현]

Departments 테이블:
| dept_id | name        | budget  |
|---------|-------------|---------|
| D001    | Engineering | 500000  |
| D002    | Marketing   | 300000  |

Employees 테이블:
| emp_id | name    | role      |
|--------|---------|-----------|
| E001   | Alice   | Engineer  |
| E002   | Bob     | Engineer  |
| E003   | Charlie | Marketer  |

Employee_Departments 테이블 (다대다 관계):
| emp_id | dept_id |
|--------|---------|
| E001   | D001    |
| E002   | D001    |
| E002   | D002    |  ← Bob은 두 부서 소속 (중복 없이!)
| E003   | D002    |

Projects 테이블:
| proj_id | name          | status   | emp_id |
|---------|---------------|----------|--------|
| P001    | Project Alpha | active   | E001   |
| P002    | Project Beta  | planning | E002   |
| P003    | Campaign X    | active   | E003   |

[장점]
1. 데이터 중복 없음 (정규화)
2. 접근 경로 독립적 - 어떤 테이블에서든 시작 가능
3. 구조 변경이 쿼리에 영향을 주지 않음 (데이터 독립성)
4. SQL로 복잡한 쿼리도 간단하게:
   SELECT p.name FROM Projects p
   JOIN Employees e ON p.emp_id = e.emp_id
   WHERE e.name = 'Bob';
""")


if __name__ == "__main__":
    print("=" * 70)
    print("계층적 데이터베이스 모델의 한계")
    print("Codd (1970) 논문 기반 분석")
    print("=" * 70)

    demonstrate_data_independence_violation()
    demonstrate_data_redundancy()
    demonstrate_query_limitations()
    compare_with_relational()

    print("\n" + "=" * 70)
    print("결론")
    print("=" * 70)
    print("""
Codd는 계층적 모델의 다음 문제점들을 지적했습니다:

1. 접근 경로 의존성 (Access Path Dependence)
   - 물리적 저장 구조에 따라 쿼리 방식이 결정됨
   - 구조 변경 시 모든 애플리케이션 코드 수정 필요

2. 데이터 중복 (Data Redundancy)
   - 다대다 관계 표현을 위해 레코드 복제 필요
   - 일관성 유지 비용 증가

3. 논리적 독립성 부재
   - 비즈니스 로직이 물리적 구조에 종속

관계형 모델은 이러한 문제를 해결하기 위해:
- 데이터를 관계(테이블)로 표현
- 수학적 집합 이론 기반의 연산 정의
- 물리적 저장과 논리적 접근을 분리
""")
