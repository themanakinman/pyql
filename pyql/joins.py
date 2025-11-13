"""
Join Operations
Handles merging DataFrames
"""


class JoinMixin:
    """Mixin for join operations"""
    
    def merge(self, other, left_on, right_on, how='inner'):
        """
        Merge with another DataFrame
        
        Args:
            other: DataFrame to merge with
            left_on: column name in self to join on
            right_on: column name in other to join on
            how: join type ('inner', 'left', 'right', 'outer')
        
        Returns:
            Merged DataFrame
        """
        if left_on not in self.columns:
            raise KeyError(f"Column '{left_on}' not found in left DataFrame")
        if right_on not in other.columns:
            raise KeyError(f"Column '{right_on}' not found in right DataFrame")
        
        if how == 'inner':
            return self._inner_join(other, left_on, right_on)
        elif how == 'left':
            return self._left_join(other, left_on, right_on)
        elif how == 'right':
            return self._right_join(other, left_on, right_on)
        elif how == 'outer':
            return self._outer_join(other, left_on, right_on)
        else:
            raise ValueError(f"Unknown join type: {how}")
    
    def _inner_join(self, other, left_on, right_on):
        """Inner join - only matching rows"""
        # Build index for right DataFrame
        right_index = {}
        for i, value in enumerate(other.data[right_on]):
            if value not in right_index:
                right_index[value] = []
            right_index[value].append(i)
        
        # Build result
        result_data = {col: [] for col in self.columns}
        
        # Add columns from right (avoid duplicates)
        for col in other.columns:
            if col not in result_data:
                result_data[col] = []
        
        result_columns = self.columns + [col for col in other.columns if col not in self.columns]
        
        # Perform join
        for i, left_value in enumerate(self.data[left_on]):
            if left_value in right_index:
                for j in right_index[left_value]:
                    # Add left row
                    for col in self.columns:
                        result_data[col].append(self.data[col][i])
                    
                    # Add right row
                    for col in other.columns:
                        if col not in self.columns:
                            result_data[col].append(other.data[col][j])
        
        from .dataframe import DataFrame
        df = DataFrame.__new__(DataFrame)
        df.data = result_data
        df.columns = result_columns
        return df
    
    def _left_join(self, other, left_on, right_on):
        """Left join - all left rows, matching right rows"""
        right_index = {}
        for i, value in enumerate(other.data[right_on]):
            if value not in right_index:
                right_index[value] = []
            right_index[value].append(i)
        
        result_data = {col: [] for col in self.columns}
        for col in other.columns:
            if col not in result_data:
                result_data[col] = []
        
        result_columns = self.columns + [col for col in other.columns if col not in self.columns]
        
        for i, left_value in enumerate(self.data[left_on]):
            if left_value in right_index:
                for j in right_index[left_value]:
                    for col in self.columns:
                        result_data[col].append(self.data[col][i])
                    for col in other.columns:
                        if col not in self.columns:
                            result_data[col].append(other.data[col][j])
            else:
                # No match - add left row with None for right columns
                for col in self.columns:
                    result_data[col].append(self.data[col][i])
                for col in other.columns:
                    if col not in self.columns:
                        result_data[col].append(None)
        
        from .dataframe import DataFrame
        df = DataFrame.__new__(DataFrame)
        df.data = result_data
        df.columns = result_columns
        return df
    
    def _right_join(self, other, left_on, right_on):
        """Right join - swap and do left join"""
        return other._left_join(self, right_on, left_on)
    
    def _outer_join(self, other, left_on, right_on):
        """Outer join - all rows from both"""
        # Do left join first
        left_result = self._left_join(other, left_on, right_on)
        
        # Find right rows not in left
        left_values = set(self.data[left_on])
        right_only_indices = [
            i for i, val in enumerate(other.data[right_on])
            if val not in left_values
        ]
        
        # Add right-only rows
        for i in right_only_indices:
            for col in self.columns:
                if col in other.columns:
                    left_result.data[col].append(other.data[col][i])
                else:
                    left_result.data[col].append(None)
            
            for col in other.columns:
                if col not in self.columns:
                    left_result.data[col].append(other.data[col][i])
        
        return left_result
