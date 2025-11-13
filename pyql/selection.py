"""
Selection and Projection Operations
Handles column selection and indexing
"""


class SelectionMixin:
    """Mixin for selection and projection operations"""
    
    def __getitem__(self, key):
        """
        Support multiple access patterns:
        - df['column'] -> list
        - df[['col1', 'col2']] -> DataFrame
        - df[BooleanMask] -> filtered DataFrame
        """
        from .filters import BooleanMask
        
        # Single column selection
        if isinstance(key, str):
            if key not in self.columns:
                raise KeyError(f"Column '{key}' not found")
            return self.data[key]
        
        # Multiple column selection (projection)
        elif isinstance(key, list):
            new_data = {}
            for col in key:
                if col not in self.columns:
                    raise KeyError(f"Column '{col}' not found")
                new_data[col] = self.data[col][:]
            
            # Create new DataFrame instance
            from .dataframe import DataFrame
            df = DataFrame.__new__(DataFrame)
            df.data = new_data
            df.columns = key
            return df
        
        # Boolean indexing (filtering)
        elif isinstance(key, BooleanMask):
            return self._filter_by_mask(key.mask)
        
        else:
            raise TypeError(f"Invalid indexing type: {type(key)}")
    
    def select(self, *columns):
        """
        Select specific columns
        
        Args:
            *columns: column names to select
        
        Returns:
            DataFrame with selected columns
        """
        return self[list(columns)]
    
    def drop(self, *columns):
        """
        Drop specific columns
        
        Args:
            *columns: column names to drop
        
        Returns:
            DataFrame without dropped columns
        """
        remaining_cols = [col for col in self.columns if col not in columns]
        return self[remaining_cols]
    
    def rename(self, column_map):
        """
        Rename columns
        
        Args:
            column_map: dict mapping old names to new names
        
        Returns:
            DataFrame with renamed columns
        """
        new_data = {}
        new_columns = []
        
        for col in self.columns:
            new_name = column_map.get(col, col)
            new_data[new_name] = self.data[col][:]
            new_columns.append(new_name)
        
        from .dataframe import DataFrame
        df = DataFrame.__new__(DataFrame)
        df.data = new_data
        df.columns = new_columns
        return df
