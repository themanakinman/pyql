class BooleanMask:
    """helper class for boolean operations"""
    
    def __init__(self, mask):
        """
        initialize boolean mask
        
        args:
            mask: list of boolean values
        """
        self.mask = mask
    
    def __and__(self, other):
        """bitwise and (&)"""
        if len(self.mask) != len(other.mask):
            raise ValueError("Masks must have same length")
        return BooleanMask([a and b for a, b in zip(self.mask, other.mask)])
    
    def __or__(self, other):
        """bitwise or (|)"""
        if len(self.mask) != len(other.mask):
            raise ValueError("Masks must have same length")
        return BooleanMask([a or b for a, b in zip(self.mask, other.mask)])
    
    def __invert__(self):
        """bitwise not (~)"""
        return BooleanMask([not x for x in self.mask])
    
    def __len__(self):
        """return length of mask"""
        return len(self.mask)


def compare(df, column, operator, value):
    """
    helper function to create comparison masks
    
    args:
        df: dataframe
        column: column name
        operator: comparison operator
        value: comparison value
    
    returns:
        booleanmask
    """
    return df._createMask(column, operator, value)
