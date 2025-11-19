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
        currentVal = ''
        inQuotes = False
        
        for ch in line:
            if ch == '"':
                inQuotes = not inQuotes # char is inside quotes
            elif ch == self.delimiter and not in_quotes:
                values.append(self._convert_type(currentVal))
                currentVal = ''
            else:
                currentVal += ch
        
        # add the final val
        values.append(self._convert_type(currentVal.strip()))
        return values
    
    def _convert_type(self, value):
        """Convert string to appropriate type"""
        value = value.strip()
        
        # remove quotes if they exist
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        
        try:
            return int(value)
        except ValueError:
            pass
        
        try:
            return float(value)
        except ValueError:
            pass
        
        # otherwise return a string
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
