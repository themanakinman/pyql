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
    



def compare(df, column, operator, value):
    """
    helper func for booleanMask
    """
    return df._create_mask(column, operator, value)
