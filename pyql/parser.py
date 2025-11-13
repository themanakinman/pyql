"""
CSV Parser Module
Handles reading and parsing CSV files
"""

class CSVParser:
    def __init__(self, filepath, delimiter=',', columns=None):
        """
        Initialize CSV Parser
        
        Args:
            filepath: path to CSV file
            delimiter: character separating values
            columns: if None, use first line as headers
        """
        self.filepath = filepath
        self.delimiter = delimiter
        self.columns = columns
    
    def parse_line(self, line):
        """Parse a single line into values"""
        values = []
        current_value = ''
        in_quotes = False
        
        for ch in line:
            if ch == '"':
                in_quotes = not in_quotes
            elif ch == self.delimiter and not in_quotes:
                values.append(self._convert_type(current_value.strip()))
                current_value = ''
            else:
                current_value += ch
        
        # Add last value
        values.append(self._convert_type(current_value.strip()))
        return values
    
    def _convert_type(self, value):
        """Convert string to appropriate type"""
        value = value.strip()
        
        # Remove quotes
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        return value
    
    def read_csv(self):
        """
        Read and parse CSV file
        
        Returns:
            tuple: (columns, data) where data is list of lists
        """
        with open(self.filepath, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        
        if not lines:
            return [], []
        
        # Get column names
        if self.columns is None:
            columns = self.parse_line(lines[0])
            start = 1
        else:
            columns = self.columns
            start = 0
        
        # Parse data rows
        data = []
        for line in lines[start:]:
            line = line.strip()
            if line:
                row = self.parse_line(line)
                data.append(row)
        
        return columns, data
