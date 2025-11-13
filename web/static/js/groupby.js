// ========================================
// GROUPBY ACTION - Data Aggregation
// ========================================

// Validate groupby input
validateGroupByInput = function(input) {
    if (!loadedDataFrame) {
        return { valid: false, error: 'Please load data first' };
    }
    
    // Parse syntax: df.groupby(Column).func(AggColumn)
    // Examples: df.groupby(Continent).sum(Population)
    //           df.groupby(Country).max(GNP)
    const groupbyRegex = /^\w*\.groupby\(([^)]+)\)\.(sum|mean|max|min|count)\(([^)]+)\)$/;
    const match = input.match(groupbyRegex);
    
    if (!match) {
        return { 
            valid: false, 
            error: 'Invalid syntax. Use: file_name.groupby(Column).sum(AggColumn)' 
        };
    }
    
    const [, groupCol, func, aggCol] = match;
    
    // Validate columns exist
    if (!loadedDataFrame.columns.includes(groupCol)) {
        return { 
            valid: false, 
            error: `Column "${groupCol}" not found` 
        };
    }
    
    if (!loadedDataFrame.columns.includes(aggCol)) {
        return { 
            valid: false, 
            error: `Column "${aggCol}" not found` 
        };
    }
    
    return { valid: true };
};

function extractDataframeName(input) {
    const match = input.match(/^(\w+)\./);
    return match ? match[1] : 'df';
}

// Execute groupby
executeGroupBy = async function(input) {
    showLoading('Aggregating data...');

    const groupbyRegex = /^\w*\.groupby\(([^)]+)\)\.(sum|mean|max|min|count)\(([^)]+)\)$/;
    const match = input.match(groupbyRegex);
    
    if (!match) {
        return { 
            valid: false, 
            error: 'Invalid syntax. Use: file_name.groupby(Column).sum(AggColumn)' 
        };
    }
    
    const dfName = extractDataframeName(input);
    
    const [, groupCol, func, aggCol] = match;
    
    try {
        const response = await fetch('/api/aggregate', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                dataframe: dfName,
                groupby: groupCol,
                column: aggCol,
                function: func
            })
        });
        
        const data = await response.json();
        
        if (!response.ok) {
            showError(data.error || 'Failed to aggregate data');
            return;
        }
        
        displayGroupByResults(data, groupCol, aggCol, func);
        
    } catch (error) {
        showError('Error aggregating data: ' + error.message);
    }
};

// Display groupby results
function displayGroupByResults(data, groupCol, aggCol, func) {
    const resultsContainer = document.getElementById('results');
    const tableHTML = createDataTable(data.data);
    
    const operation = `${func.toUpperCase()}(${aggCol}) GROUP BY ${groupCol}`;
    
    resultsContainer.innerHTML = `
        <div class="data-card">
            <div class="data-header">
                <h3>✓ Data Aggregated</h3>
                <span class="dataset-name">${data.rows.toLocaleString()} groups</span>
            </div>
            
            <div class="data-section">
                <h4>Operation</h4>
                <code class="query-display">${escapeHtml(operation)}</code>
            </div>
            
            <div class="data-section">
                <h4>Results</h4>
                ${tableHTML}
            </div>
            
            <div class="data-actions">
                <button class="secondary-btn" onclick="clearResults()">New Query</button>
            </div>
        </div>
    `;
}
