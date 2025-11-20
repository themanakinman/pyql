class CSVParser:
    def __init__(self, filepath, delimiter=None, columns=None):
        """
        init the csv parser
        """
        self.filepath = filepath
        self.delimiter = delimiter
        self.columns = columns
    
    def _parseLine(self, line):
        """
        parse a single line into vals
        """
        values = []
        currentVal = ''
        inQuotes = False
        
        for ch in line:
            if ch == '"':
                inQuotes = not inQuotes # char is inside quotes
            elif ch == self.delimiter and not inQuotes:
                values.append(self._convertType(currentVal))
                currentVal = ''
            else:
                currentVal += ch
        
        # add the final val
        values.append(self._convertType(currentVal.strip()))
        return values
    
    def _convertType(self, value):
        """
        convert string to the appropriate type
        """
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
    
    def readCSV(self):
        """
        read and parse csv file
        """
        with open(self.filepath, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        
        if not lines:
            return [], []
        
        if self.columns is None:
            columns = self._parseLine(lines[0])
            start = 1
        else:
            columns = self.columns
            start = 0
        
        data = []
        for line in lines[start:]:
            line = line.strip()
            if line:
                row = self._parseLine(line)
                data.append(row)
        
        return columns, data
