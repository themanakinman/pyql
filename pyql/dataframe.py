from .parser import CSVParser
from .boolean_mask import BooleanMask
from .group_by import GroupBy

class DataFrame():
    """
    core dataframe class that stores data in column-oriented format
    """
    def __init__(self, data=dict(list()), columns=list()):
        """
        initialize the dataframe object
        """
        self.data = {}
        self.columns = []
        
        if data is not None:
            if isinstance(data, dict):
                self.data = {k: list(v) for k, v in data.items()}
                self.columns = list(data.keys())
            elif isinstance(data, list) and columns is not None: # row oriented data, re-orient
                self.columns = columns
                for col in columns:
                    self.data[col] = []
                
                for row in data:
                    for i, value in enumerate(row):
                        if i < len(columns):
                            self.data[columns[i]].append(value)
            else:
                raise ValueError("Invalid data format")
    
    @classmethod
    def fromCSV(cls, filepath, delimiter=None, columns=None):
        """
        create a dataframe from a csv file
        """
        parser = CSVParser(filepath, delimiter, columns)
        cols, data = parser.readCSV()
        return cls(data=data, columns=cols)
    
    def __str__(self):
        """
        return a string representation of the dataframe
        """
        if not self.columns:
            return "No columns found. The dataframe is empty."
        
        header = "[" + " | ".join(self.columns) + "]"
        
        numRows = len(self)
        rows = []
        displayRows = min(numRows, 10)
        
        for i in range(displayRows):
            rowValues = [str(self.data[col][i]) for col in self.columns]
            rows.append(" | ".join(rowValues))
        
        result = f"{header}\n" + "\n".join(rows)
        
        if numRows > displayRows:
            result += f"\n... ({numRows - displayRows} more rows)"
        
        result += f"\n\n[{numRows} rows x {len(self.columns)} columns]"
        return result
    
    def __len__(self):
        """
        return number of rows
        """
        if not self.columns:
            return 0
        return len(list(self.data.values())[0])

    def shape(self):
        """
        return (rows, columns) tuple
        """
        return (len(self), len(self.columns))
    
    def head(self, n=5):
        """
        return first n rows
        """
        newData = {}
        for col in self.columns:
            newData[col] = self.data[col][:n]
        return DataFrame(data=newData)
    
    """
    copy funcs
    """
    
    def copy(self):
        """
        return a deep copy of dataframe
        """
        newData = {col: self.data[col][:] for col in self.columns}
        return DataFrame(data=newData)

    def toDict(self):
        """
        convert dataframe to dictionary
        """
        return {col: self.data[col][:] for col in self.columns}
    
    def toList(self):
        """
        convert dataframe to list of lists (rows)
        """
        rows = []
        for i in range(len(self)):
            row = [self.data[col][i] for col in self.columns]
            rows.append(row)
        return rows
    
    """
    aggregation funcs
    """
    
    def sum(self, column):
        """
        sum of column values
        """
        if column not in self.columns:
            raise KeyError(f"Column '{column}' not found")
        return sum(self.data[column])
    
    def mean(self, column):
        """
        average of column values
        """
        if column not in self.columns:
            raise KeyError(f"Column '{column}' not found")
        values = self.data[column]
        return sum(values) / len(values) if values else 0
    
    def max(self, column):
        """
        max of column values
        """
        if column not in self.columns:
            raise KeyError(f"Column '{column}' not found")
        return max(self.data[column])
    
    def min(self, column):
        """
        min of column values
        """
        if column not in self.columns:
            raise KeyError(f"Column '{column}' not found")
        return min(self.data[column])
    
    def count(self, column):
        """
        count of non-null values
        """
        if column not in self.columns:
            raise KeyError(f"Column '{column}' not found")
        return len(self.data[column])
    
    def groupBy(self, byColumn):
        """
        group dataframe by column        
        """
        if byColumn not in self.columns:
            raise KeyError(f"Column '{byColumn}' not found")
        
        return GroupBy(self, byColumn)
    
    """
    filter funcs
    """
    
    def _filterByMask(self, mask):
        """
        filter rows based on boolean mask
        """
        if len(mask) != len(self):
            raise ValueError("Mask length must match DataFrame length")
        
        newData = {}
        for col in self.columns:
            newData[col] = [self.data[col][i] for i in range(len(mask)) if mask[i]]
        
        df = DataFrame.__new__(DataFrame)
        df.data = newData
        df.columns = self.columns[:]
        return df
    
    def filter(self, column, operator, value):
        """
        filter rows based on condition
        """
        mask = self._createMask(column, operator, value)
        return self[mask]
    
    def _createMask(self, column, operator, value):
        """
        create boolean mask from comparison (for boolean_mask.py)
        """
        if column not in self.columns:
            raise KeyError(f"Column '{column}' not found")
        
        colData = self.data[column]
        mask = []
        
        for val in colData:
            if operator == '>':
                mask.append(val > value)
            elif operator == '>=':
                mask.append(val >= value)
            elif operator == '<':
                mask.append(val < value)
            elif operator == '<=':
                mask.append(val <= value)
            elif operator == '==':
                mask.append(val == value)
            elif operator == '!=':
                mask.append(val != value)
            else:
                raise ValueError(f"Unknown operator: {operator}")
        
        return BooleanMask(mask)
    
    """
    selection funcs
    """
    
    def __getitem__(self, key):
        """
        supports multiple access patterns such as:
        - df['column'] -> list
        - df[['col1', 'col2']] -> dataframe
        - df[booleanmask] -> filtered dataframe
        """
        if isinstance(key, str): # single column
            if key not in self.columns:
                raise KeyError(f"Column '{key}' not found")
            return self.data[key]
        
        elif isinstance(key, list): # list of columns
            newData = {}
            for col in key:
                if col not in self.columns:
                    raise KeyError(f"Column '{col}' not found")
                newData[col] = self.data[col][:]
            
            df = DataFrame.__new__(DataFrame)
            df.data = newData
            df.columns = key
            return df
        
        elif isinstance(key, BooleanMask): # filter column
            return self._filterByMask(key.mask)
        
        else:
            raise TypeError(f"Invalid indexing type: {type(key)}")
    
    def select(self, *columns):
        """
        select specific columns
        """
        return self[list(columns)]
    
    def drop(self, *columns):
        """
        drop specific columns
        """
        remainingCols = [col for col in self.columns if col not in columns]
        return self[remainingCols]
    
    def rename(self, columnMap):
        """
        rename columns
        """
        newData = {}
        newColumns = []
        
        for col in self.columns:
            newName = columnMap.get(col, col)
            newData[newName] = self.data[col][:]
            newColumns.append(newName)
        
        df = DataFrame.__new__(DataFrame)
        df.data = newData
        df.columns = newColumns
        return df
    
    """
    join funcs
    """
    
    def merge(self, other, leftOn, rightOn, how='inner'):
        """
        merge with another dataframe
        """
        if leftOn not in self.columns:
            raise KeyError(f"Column '{leftOn}' not found in left DataFrame")
        if rightOn not in other.columns:
            raise KeyError(f"Column '{rightOn}' not found in right DataFrame")
        
        if how == 'inner':
            return self._innerJoin(other, leftOn, rightOn)
        elif how == 'left':
            return self._leftJoin(other, leftOn, rightOn)
        elif how == 'right':
            return self._rightJoin(other, leftOn, rightOn)
        elif how == 'outer':
            return self._outerJoin(other, leftOn, rightOn)
        else:
            raise ValueError(f"Unknown join type: {how}")
    
    def _innerJoin(self, other, leftOn, rightOn):
        """
        inner join - only matching rows
        """
        # build index for right dataframe fuirst
        rightIndex = {}
        for i, value in enumerate(other.data[rightOn]):
            if value not in rightIndex:
                rightIndex[value] = []
            rightIndex[value].append(i)
        
        resultData = {col: [] for col in self.columns}
        
        # add columns from right (no dupes)
        for col in other.columns:
            if col not in resultData:
                resultData[col] = []
        
        resultColumns = self.columns + [col for col in other.columns if col not in self.columns]
        
        # join dat
        for i, leftValue in enumerate(self.data[leftOn]):
            if leftValue in rightIndex:
                for j in rightIndex[leftValue]:
                    for col in self.columns:
                        resultData[col].append(self.data[col][i])
                    
                    for col in other.columns:
                        if col not in self.columns:
                            resultData[col].append(other.data[col][j])
        
        df = DataFrame.__new__(DataFrame)
        df.data = resultData
        df.columns = resultColumns
        return df
    
    def _leftJoin(self, other, leftOn, rightOn):
        """
        left join - all left rows, matching right rows
        """
        rightIndex = {}
        for i, value in enumerate(other.data[rightOn]):
            if value not in rightIndex:
                rightIndex[value] = []
            rightIndex[value].append(i)
        
        resultData = {col: [] for col in self.columns}
        for col in other.columns:
            if col not in resultData:
                resultData[col] = []
        
        resultColumns = self.columns + [col for col in other.columns if col not in self.columns]
        
        for i, leftValue in enumerate(self.data[leftOn]):
            if leftValue in rightIndex:
                for j in rightIndex[leftValue]:
                    for col in self.columns:
                        resultData[col].append(self.data[col][i])
                    for col in other.columns:
                        if col not in self.columns:
                            resultData[col].append(other.data[col][j])
            else:
                # no match - add left row with none for right columns
                for col in self.columns:
                    resultData[col].append(self.data[col][i])
                for col in other.columns:
                    if col not in self.columns:
                        resultData[col].append(None)
        
        df = DataFrame.__new__(DataFrame)
        df.data = resultData
        df.columns = resultColumns
        return df
    
    def _rightJoin(self, other, leftOn, rightOn):
        """
        right join - swap and do left join
        """
        return other._leftJoin(self, rightOn, leftOn)
    
    def _outerJoin(self, other, leftOn, rightOn):
        """
        outer join - all rows from both
        """
        leftResult = self._leftJoin(other, leftOn, rightOn)
        
        leftValues = set(self.data[leftOn])
        rightOnlyIndices = [
            i for i, val in enumerate(other.data[rightOn])
            if val not in leftValues
        ]
        
        # add right-only rows
        for i in rightOnlyIndices:
            for col in self.columns:
                if col in other.columns:
                    leftResult.data[col].append(other.data[col][i])
                else:
                    leftResult.data[col].append(None)
            
            for col in other.columns:
                if col not in self.columns:
                    leftResult.data[col].append(other.data[col][i])
        
        return leftResult
