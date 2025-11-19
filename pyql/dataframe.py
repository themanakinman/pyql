from .parser import CSVParser
class DataFrame():
    """
    consists of the core dataframe class which:
    1. stores data in column-oriented format
    2. other cool stuf
    """
    
    def __init__(self, data=dict(list()), columns=list()):
        """
        init the dataframe object
        """
        self.data = {}
        self.columns = []
        
        if data is not None: # non empty data
            if isinstance(data, dict): # column oriented data
                self.data = {k: list(v) for k, v in data.items()} # data is the key (column name) and value (list of values)
                self.columns = list(data.keys()) # column mames are the keys
            else:
                raise ValueError("Data must be a dictionary of lists")
    
    @classmethod
    def from_csv(cls, filepath, delimiter=',', columns=None):
        """
        make a dataframe object out of a csv file
        """
        parser = CSVParser(filepath, delimiter, columns) # create parser object
        cols, data = parser.read_csv()
        return cls(data=data, columns=cols) # instantiate a dataframe object from the returned data and columns
    
    def __str__(self):
        """
        return a string version of the dataframe object
        """
        if not self.columns:
            return "No columns found. The dataframe is empty."
        
        header = "[" + " | ".join(self.columns) + "]"
        
        num_rows = len(self)
        rows = []
        display_rows = min(num_rows, 10)
        
        for i in range(display_rows):
            row_values = [str(self.data[col][i]) for col in self.columns]
            rows.append(" | ".join(row_values))
        
        result = f"{header}\n" + "\n".join(rows)
        
        if num_rows > display_rows:
            result += f"\n... ({num_rows - display_rows} more rows)"
        
        result += f"\n\n[{num_rows} rows x {len(self.columns)} columns]"
        return result
    
    def __len__(self):
        """return num of rows"""
        if not self.columns:
            return 0
        return len(list(self.data.values())[0])
    
    def shape(self):
        """Return (rows, columns)"""
        return (len(self), len(self.columns))
    
    def head(self, n=5):
        """Return first n rows"""
        new_data = {}
        for col in self.columns:
            new_data[col] = self.data[col][:n]
        return DataFrame(data=new_data)
    
    def copy(self):
        """Return a deep copy of DataFrame"""
        new_data = {col: self.data[col][:] for col in self.columns}
        return DataFrame(data=new_data)
    
    def to_dict(self):
        """Convert DataFrame to dictionary"""
        return {col: self.data[col][:] for col in self.columns}
    
    def to_list(self):
        """Convert DataFrame to list of lists (rows)"""
        rows = []
        for i in range(len(self)):
            row = [self.data[col][i] for col in self.columns]
            rows.append(row)
        return rows

"""
aggregation funcs
"""

"""
filter funcs
"""

"""
join funcs
"""

"""
parser funcs
"""

"""
selection funcs
"""