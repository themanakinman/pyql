from pyql.filters import FilterMixin
from pyql.dataframe import DataFrame

# Create a test DataFrame
data = {
    'name': ['Alice', 'Bob', 'Charlie', 'David'],
    'age': [25, 30, 35, 40],
    'score': [85, 92, 78, 88]
}

df = DataFrame(data)

# Test 1: Filter age greater than 30
print("People older than 30:")
result = df.filter('age', '>', 30)
print(result)

# Test 2: Filter score less than or equal to 85
print("\nPeople with score <= 85:")
result = df.filter('score', '<=', 85)
print(result)

# Test 3: Using the compare helper function
print("\nUsing compare function to filter names:")
from pyql.filters import compare
mask = compare(df, 'name', '==', 'Alice') | compare(df, 'name', '==', 'Bob')
result = df[mask]
print(result)