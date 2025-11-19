from pyql.filters import FilterMixin
from pyql.dataframe import DataFrame

# Create a test DataFrame
data = {
    'name': ['Alice', 'Bob', 'Charlie', 'David'],
    'age': [25, 30, 35, 40],
    'score': [85, 92, 78, 88]
}

falseData = ['name', 'age', 'score']

df = DataFrame(data)
first = len(list(df.data.values())[0])
print(len(first))

# df1 = DataFrame(falseData)
 