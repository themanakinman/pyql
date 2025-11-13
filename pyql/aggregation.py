"""
Aggregation and GroupBy Operations
"""


class AggregationMixin:
    """Mixin for aggregation operations"""
    
    def sum(self, column):
        """Sum of column values"""
        if column not in self.columns:
            raise KeyError(f"Column '{column}' not found")
        return sum(self.data[column])
    
    def mean(self, column):
        """Mean of column values"""
        if column not in self.columns:
            raise KeyError(f"Column '{column}' not found")
        values = self.data[column]
        return sum(values) / len(values) if values else 0
    
    def max(self, column):
        """Maximum of column values"""
        if column not in self.columns:
            raise KeyError(f"Column '{column}' not found")
        return max(self.data[column])
    
    def min(self, column):
        """Minimum of column values"""
        if column not in self.columns:
            raise KeyError(f"Column '{column}' not found")
        return min(self.data[column])
    
    def count(self, column):
        """Count of non-null values"""
        if column not in self.columns:
            raise KeyError(f"Column '{column}' not found")
        return len(self.data[column])
    
    def groupby(self, by_column):
        """
        Group DataFrame by column
        
        Args:
            by_column: column name to group by
        
        Returns:
            GroupBy object
        """
        if by_column not in self.columns:
            raise KeyError(f"Column '{by_column}' not found")
        
        return GroupBy(self, by_column)


class GroupBy:
    """GroupBy object for aggregation operations"""
    
    def __init__(self, df, by_column):
        """
        Initialize GroupBy
        
        Args:
            df: DataFrame to group
            by_column: column to group by
        """
        self.df = df
        self.by_column = by_column
        self._groups = self._create_groups()
    
    def _create_groups(self):
        """Create dictionary of groups"""
        groups = {}
        by_values = self.df.data[self.by_column]
        
        for i, value in enumerate(by_values):
            if value not in groups:
                groups[value] = []
            groups[value].append(i)
        
        return groups
    
    def agg(self, agg_dict):
        """
        Perform aggregation
        
        Args:
            agg_dict: dict mapping column names to aggregation functions
                     e.g., {'GNP': 'max', 'Population': 'sum'}
        
        Returns:
            DataFrame with aggregated results
        """
        result_data = {self.by_column: []}
        
        # Initialize result columns
        for col in agg_dict.keys():
            result_data[col] = []
        
        # Perform aggregation for each group
        for group_value, indices in self._groups.items():
            result_data[self.by_column].append(group_value)
            
            for col, func_name in agg_dict.items():
                if col not in self.df.columns:
                    raise KeyError(f"Column '{col}' not found")
                
                # Get values for this group
                values = [self.df.data[col][i] for i in indices]
                
                # Apply aggregation function
                if func_name == 'sum':
                    result = sum(values)
                elif func_name == 'mean' or func_name == 'avg':
                    result = sum(values) / len(values)
                elif func_name == 'max':
                    result = max(values)
                elif func_name == 'min':
                    result = min(values)
                elif func_name == 'count':
                    result = len(values)
                else:
                    raise ValueError(f"Unknown aggregation function: {func_name}")
                
                result_data[col].append(result)
        
        from .dataframe import DataFrame
        return DataFrame(data=result_data)
    
    def sum(self):
        """Sum all numeric columns"""
        agg_dict = {}
        for col in self.df.columns:
            if col != self.by_column:
                agg_dict[col] = 'sum'
        return self.agg(agg_dict)
    
    def mean(self):
        """Mean of all numeric columns"""
        agg_dict = {}
        for col in self.df.columns:
            if col != self.by_column:
                agg_dict[col] = 'mean'
        return self.agg(agg_dict)
    
    def max(self):
        """Max of all numeric columns"""
        agg_dict = {}
        for col in self.df.columns:
            if col != self.by_column:
                agg_dict[col] = 'max'
        return self.agg(agg_dict)
