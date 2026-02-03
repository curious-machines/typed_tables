"""Example usage of the typed_tables library."""

from pathlib import Path
from typed_tables import Schema

# Define a data structure of types using the DSL
types = """
define uuid as uint128
define name as character[]
define age as uint8

Person {
    id: uuid
    name
    age
}
"""

# Create a data directory for storage
data_dir = Path("./example_data")

# Parse the schema and build an in-memory representation
with Schema.parse(types, data_dir) as schema:
    # Create several Person instances
    people = [
        {"id": 0x00000001_00000000_00000000_00000001, "name": list("Alice"), "age": 30},
        {"id": 0x00000002_00000000_00000000_00000002, "name": list("Bob"), "age": 25},
        {"id": 0x00000003_00000000_00000000_00000003, "name": list("Charlie"), "age": 35},
        {"id": 0x00000004_00000000_00000000_00000004, "name": list("Diana"), "age": 28},
        {"id": 0x00000005_00000000_00000000_00000005, "name": list("Eve"), "age": 22},
        {"id": 0x00000006_00000000_00000000_00000006, "name": list("Frank"), "age": 45},
        {"id": 0x00000007_00000000_00000000_00000007, "name": list("Grace"), "age": 30},
        {"id": 0x00000008_00000000_00000000_00000008, "name": list("Henry"), "age": 55},
        {"id": 0x00000009_00000000_00000000_00000009, "name": list("Ivy"), "age": 19},
        {"id": 0x0000000A_00000000_00000000_0000000A, "name": list("Jack"), "age": 30},
    ]

    print("Creating Person instances...")
    for person_data in people:
        person = schema.create_instance("Person", person_data)
        print(f"  Created: {person}")

    # Load and display all people
    print("\nAll people in database:")
    for i in range(len(people)):
        ref = schema.get_instance("Person", i)
        data = ref.load()
        print(f"  [{i}] {data['name']}, age {data['age']}")

    # Show files created
    print(f"\nFiles created in {data_dir}:")
    for f in sorted(data_dir.iterdir()):
        print(f"  {f.name} ({f.stat().st_size} bytes)")

    print("\n" + "=" * 60)
    print("You can now query this data using the TTQ REPL:")
    print(f"  ttq {data_dir}")
    print("\nExample queries:")
    print("  from Person select *")
    print("  from Person select * where age >= 30")
    print("  from Person select * where name starts with \"A\"")
    print("  from Person select name, age sort by age")
    print("  from Person select age, count() group by age")
    print("  from Person select average(age)")
