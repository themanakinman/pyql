class GroupBy:
    
    def __init__(self, df, by_column):
        """
        init GroupBy
        """
        self.df = df
        self.by_column = by_column
        self._groups = self._create_groups()
    
    def _create_groups(self):
        """create dictionary of groups"""
        groups = {}
        by_values = self.df.data[self.by_column]
        
        for i, value in enumerate(by_values):
            if value not in groups:
                groups[value] = []
            groups[value].append(i)
        
        return groups
    
    def agg(self, agg_dict):
        """
        perform aggregation
        """
        result_data = {self.by_column: []}
        
        # init the result columns
        for col in agg_dict.keys():
            result_data[col] = []
        
        # then perform aggregation for each group
        for group_value, indices in self._groups.items():
            result_data[self.by_column].append(group_value)
            
            for col, func_name in agg_dict.items():
                if col not in self.df.columns:
                    raise KeyError(f"Column '{col}' not found")
                
                # vals for this group
                values = [self.df.data[col][i] for i in indices]
                
                # agg the vals
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
        """sum of all numeric columns"""
        agg_dict = {}
        for col in self.df.columns:
            if col != self.by_column:
                agg_dict[col] = 'sum'
        return self.agg(agg_dict)
    
    def mean(self):
        """avgerage of all numeric columns"""
        agg_dict = {}
        for col in self.df.columns:
            if col != self.by_column:
                agg_dict[col] = 'mean'
        return self.agg(agg_dict)
    
    def max(self):
        """max of all numeric columns"""
        agg_dict = {}
        for col in self.df.columns:
            if col != self.by_column:
                agg_dict[col] = 'max'
        return self.agg(agg_dict)
