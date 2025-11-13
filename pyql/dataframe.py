
from .parser import CSVParser
from .selection import SelectionMixin
from .filters import FilterMixin
from .aggregation import AggregationMixin
from .joins import JoinMixin

class DataFrame(SelectionMixin, FilterMixin, AggregationMixin, JoinMixin):
    """
    consists of the core dataframe class which:
    1. stores data in column-oriented format
    2. implements selection, filtering, aggregation, and join operations
    3. other cool stuf
    """
    
    def __init__(self, data=None, columns=None):
        """
        init the dataframe object
        
        params:
            data: dict of lists (column-oriented) or list of lists (row-oriented)
            columns: list of column names (required if data is list of lists)
        """
        self.data = {}
        self.columns = []
        
        if data is not None: # non empty data
            if isinstance(data, dict): # column oriented data
                self.data = {k: list(v) for k, v in data.items()} # data is the key (column name) and value (list of values)
                self.columns = list(data.keys()) # column mames are the keys
            
            elif isinstance(data, list) and columns is not None: # row oriented data, re-orient
                self.columns = columns
                for col in columns:
                    self.data[col] = []
                
                for row in data:
                    for i, value in enumerate(row):
                        if i < len(columns):
                            self.data[columns[i]].append(value)
    
    @classmethod
    def from_csv(cls, filepath, delimiter=',', columns=None):
        """
        make a dataframe object out of a csv file
        
        params:
            filepath: path to CSV file
            delimiter: character separating values
            columns: custom column names (if None, read from file)
        
        return:
            dataframe instance
        """
        parser = CSVParser(filepath, delimiter, columns)
        cols, data = parser.read_csv()
        return cls(data=data, columns=cols) # instantiate a dataframe object from the returned data and columns
    
    def __repr__(self):
        """
        
        return a string version of the dataframe object
        
        """
        if not self.columns:
            return "No columns found. The dataframe is empty."
        
        # Build header
        header = " | ".join(str(col) for col in self.columns)
        separator = "-" * len(header)
        
        # Build rows
        num_rows = len(self)
        rows = []
        display_rows = min(num_rows, 10)
        
        for i in range(display_rows):
            row_values = [str(self.data[col][i]) for col in self.columns]
            rows.append(" | ".join(row_values))
        
        result = f"{header}\n{separator}\n" + "\n".join(rows)
        
        if num_rows > display_rows:
            result += f"\n... ({num_rows - display_rows} more rows)"
        
        result += f"\n\n[{num_rows} rows x {len(self.columns)} columns]"
        return result
    
    def __len__(self):
        """Return number of rows"""
        if not self.columns:
            return 0
        return len(self.data[self.columns[0]])
    
    def shape(self):
        """Return (rows, columns) tuple"""
        return (len(self), len(self.columns))
    
    def head(self, n=5):
        """Return first n rows"""
        new_data = {}
        for col in self.columns:
            new_data[col] = self.data[col][:n]
        return DataFrame(data=new_data)
    
    def tail(self, n=5):
        """Return last n rows"""
        new_data = {}
        for col in self.columns:
            new_data[col] = self.data[col][-n:]
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
