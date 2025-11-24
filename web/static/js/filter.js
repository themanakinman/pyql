
validateFilterInput = function(input) {
    if (!loadedDataFrame) {
        return { valid: false, error: 'Please load data first' };
    }
    
    // check for compound filters (with & or |)
    if (input.includes(' & ') || input.includes(' | ')) {
        const parsed = parseCompoundFilter(input);
        if (!parsed) {
            return { 
                valid: false, 
                error: 'Invalid compound filter. Use: file_name[Column1] > 100 & file_name[Column2] == "Value"' 
            };
        }
        return { valid: true };
    }
    
    // single filter validation
    const filterRegex = /^\w*\[([^\]]+)\]\s*([><=!]+)\s*(.+)$/;
    const match = input.match(filterRegex);
    
    if (!match) {
        return { 
            valid: false, 
            error: 'Invalid syntax. Use: file_name[Column] > 1000' 
        };
    }
    
    const [, column, operator, value] = match;
    
    if (!loadedDataFrame.columns.includes(column)) {
        return { 
            valid: false, 
            error: `Column "${column}" not found` 
        };
    }
    
    const validOperators = ['>', '>=', '<', '<=', '==', '!='];
    if (!validOperators.includes(operator)) {
        return { 
            valid: false, 
            error: `Invalid operator "${operator}"` 
        };
    }
    
    return { valid: true };
};

function extractDataframeName(input) {
    const match = input.match(/^(\w+)\[/);
    return match ? match[1] : loadedDataFrame.name;
}

function parseCompoundFilter(input) {
    // determine logic operator (& or |)
    let logicOp = 'and';
    let parts = [];
    
    if (input.includes(' & ')) {
        logicOp = 'and';
        parts = input.split(' & ');
    } else if (input.includes(' | ')) {
        logicOp = 'or';
        parts = input.split(' | ');
    } else {
        return null;
    }
    
    // parse each filter part
    const filters = [];
    const filterRegex = /^\w*\[([^\]]+)\]\s*([><=!]+)\s*(.+)$/;
    
    for (const part of parts) {
        const match = part.trim().match(filterRegex);
        if (!match) return null;
        
        const [, column, operator, value] = match;
        
        // validate column exists
        if (!loadedDataFrame.columns.includes(column)) {
            return null;
        }
        
        filters.push({
            column: column,
            operator: operator,
            value: value.trim()
        });
    }
    
    return {
        filters: filters,
        logic: logicOp
    };
}

executeFilter = async function(input) {
    showLoading('Filtering data...');
    
    // check for compound filter
    const isCompound = input.includes(' & ') || input.includes(' | ');

    const dfName = extractDataframeName(input);
    
    let requestBody;
    
    if (isCompound) {
        // parse compound filter
        const parsed = parseCompoundFilter(input);
        if (!parsed) {
            showError('Invalid compound filter syntax');
            return;
        }
        
        requestBody = {
            dataframe: dfName,
            filters: parsed.filters,
            logic: parsed.logic
        };
    } else {
        // parse single filter
        const filterRegex = /\[([^\]]+)\]\s*([><=!]+)\s*(.+)$/;
        const match = input.match(filterRegex);
        
        if (!match) {
            showError('Invalid filter syntax');
            return;
        }
        
        const [, column, operator, value] = match;
        
        requestBody = {
            dataframe: dfName,
            filters: [{
                column: column,
                operator: operator,
                value: value.trim()
            }],
            logic: 'and'
        };
    }
    
    try {
        const response = await fetch('/api/filterData', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(requestBody)
        });
        
        const data = await response.json();
        
        if (!response.ok) {
            showError(data.error || 'Failed to filter data');
            return;
        }
        
        displayFilterResults(data, input);
        
    } catch (error) {
        showError('Error filtering data: ' + error.message);
    }
};

function displayFilterResults(data, query) {
    const resultsContainer = document.getElementById('results');
    const tableHTML = createDataTable(data.data);
    
    // column limitation message
    const totalCols = data.total_columns || 0;
    const displayedCols = data.displayed_columns || Object.keys(data.data).length;
    const columnNote = totalCols > displayedCols
        ? `<p class="column-info">Displaying a max of ${displayedCols} out of ${totalCols} columns for performance*</p>`
        : '';
    
    resultsContainer.innerHTML = `
        <div class="data-card">
            <div class="data-header">
                <h3>✓ Filter Applied</h3>
                <span class="dataset-name">${data.rows.toLocaleString()} rows</span>
            </div>
            
            <div class="data-section">
                <h4>Query</h4>
                <code class="query-display">${escapeHtml(query)}</code>
            </div>
            
            <div class="data-section">
                <h4>Results</h4>
                ${columnNote}
                ${tableHTML}
            </div>
            
            <div class="data-actions">
                <button class="secondary-btn" onclick="clearResults()">New Query</button>
            </div>
        </div>
    `;
}

function createDataTable(dataObj) {
    if (!dataObj || Object.keys(dataObj).length === 0) {
        return '<p class="no-data">No results found</p>';
    }
    
    const columns = Object.keys(dataObj);
    const rowCount = dataObj[columns[0]].length;
    
    if (rowCount === 0) {
        return '<p class="no-data">No rows match the filter</p>';
    }
    
    // start table with container
    let tableHTML = '<div class="table-container"><table class="data-table">';
    
    // table header
    tableHTML += '<thead><tr>';
    columns.forEach(col => {
        tableHTML += `<th>${escapeHtml(col)}</th>`;
    });
    tableHTML += '</tr></thead>';
    
    // table body
    tableHTML += '<tbody>';
    const displayRows = Math.min(rowCount, 100);
    
    for (let i = 0; i < displayRows; i++) {
        tableHTML += '<tr>';
        columns.forEach(col => {
            const value = dataObj[col][i];
            let displayValue;
            
            if (value === null || value === undefined) {
                displayValue = '<span class="null-value">null</span>';
            } else if (typeof value === 'number') {
                // format numbers nicely
                displayValue = formatNumber(value);
            } else {
                // escape html and truncate long strings
                displayValue = escapeHtml(String(value));
            }
            
            tableHTML += `<td title="${escapeHtml(String(value))}">${displayValue}</td>`;
        });
        tableHTML += '</tr>';
    }
    
    tableHTML += '</tbody></table></div>';
    
    // add note if there are more rows
    if (rowCount > 100) {
        tableHTML += `<p class="table-note">Displaying a max of 100 out of ${rowCount.toLocaleString()} rows for performance*</p>`;
    }
    
    return tableHTML;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatNumber(num) {
    if (Number.isInteger(num)) {
        return num.toLocaleString();
    } else {
        // round to 2 decimal places for floats
        return num.toLocaleString(undefined, {
            minimumFractionDigits: 0,
            maximumFractionDigits: 2
        });
    }
}
