"""
Filtering Operations
Handles boolean indexing and conditional filtering
"""


class BooleanMask:
    """Helper class for boolean operations"""
    
    def __init__(self, mask):
        """
        Initialize boolean mask
        
        Args:
            mask: list of boolean values
        """
        self.mask = mask
    
    def __and__(self, other):
        """Bitwise AND (&)"""
        if len(self.mask) != len(other.mask):
            raise ValueError("Masks must have same length")
        return BooleanMask([a and b for a, b in zip(self.mask, other.mask)])
    
    def __or__(self, other):
        """Bitwise OR (|)"""
        if len(self.mask) != len(other.mask):
            raise ValueError("Masks must have same length")
        return BooleanMask([a or b for a, b in zip(self.mask, other.mask)])
    
    def __invert__(self):
        """Bitwise NOT (~)"""
        return BooleanMask([not x for x in self.mask])
    
    def __len__(self):
        return len(self.mask)


class FilterMixin:
    """Mixin for filtering operations"""
    
    def _filter_by_mask(self, mask):
        """Filter rows based on boolean mask"""
        if len(mask) != len(self):
            raise ValueError("Mask length must match DataFrame length")
        
        new_data = {}
        for col in self.columns:
            new_data[col] = [self.data[col][i] for i in range(len(mask)) if mask[i]]
        
        from .dataframe import DataFrame
        df = DataFrame.__new__(DataFrame)
        df.data = new_data
        df.columns = self.columns[:]
        return df
    
    def filter(self, column, operator, value):
        """
        Filter rows based on condition
        
        Args:
            column: column name
            operator: comparison operator (>, <, ==, !=, >=, <=)
            value: comparison value
        
        Returns:
            Filtered DataFrame
        """
        mask = self._create_mask(column, operator, value)
        return self[mask]
    
    def _create_mask(self, column, operator, value):
        """Create boolean mask from comparison"""
        if column not in self.columns:
            raise KeyError(f"Column '{column}' not found")
        
        col_data = self.data[column]
        mask = []
        
        for val in col_data:
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


def compare(df, column, operator, value):
    """
    Helper function to create comparison masks
    
    Args:
        df: DataFrame
        column: column name
        operator: comparison operator
        value: comparison value
    
    Returns:
        BooleanMask
    """
    return df._create_mask(column, operator, value)
