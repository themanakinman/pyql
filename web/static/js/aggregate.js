// ========================================
// AGGREGATE ACTION - Simple Aggregation
// ========================================

// Validate aggregate input
validateAggregateInput = function(input) {
    if (!loadedDataFrame) {
        return { valid: false, error: 'Please load data first' };
    }
    
    // Parse syntax: df.sum(Column) or df.mean(Column) etc.
    const aggRegex = /^df\.(sum|mean|max|min|count)\(([^)]+)\)$/;
    const match = input.match(aggRegex);
    
    if (!match) {
        return { 
            valid: false, 
            error: 'Invalid syntax. Use: df.sum(Column) or df.mean(Column)' 
        };
    }
    
    const [, func, column] = match;
    
    // Validate column exists
    if (!loadedDataFrame.columns.includes(column)) {
        return { 
            valid: false, 
            error: `Column "${column}" not found. Available: ${loadedDataFrame.columns.join(', ')}` 
        };
    }
    
    return { valid: true };
};

// Execute aggregate
executeAggregate = async function(input) {
    showLoading('Calculating aggregate...');
    
    // Parse the aggregate command
    const aggRegex = /^df\.(sum|mean|max|min|count)\(([^)]+)\)$/;
    const match = input.match(aggRegex);
    
    if (!match) {
        showError('Invalid aggregate syntax');
        return;
    }
    
    const [, func, column] = match;
    
    try {
        const response = await fetch('/api/aggregate-simple', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                dataframe: 'df',
                column: column,
                function: func
            })
        });
        
        const data = await response.json();
        
        if (!response.ok) {
            showError(data.error || 'Failed to calculate aggregate');
            return;
        }
        
        displayAggregateResult(data, column, func);
        
    } catch (error) {
        showError('Error calculating aggregate: ' + error.message);
    }
};

// Display aggregate result
function displayAggregateResult(data, column, func) {
    const resultsContainer = document.getElementById('results');
    
    const operation = `${func.toUpperCase()}(${column})`;
    const result = data.result;
    
    resultsContainer.innerHTML = `
        <div class="data-card">
            <div class="data-header">
                <h3>✓ Aggregate Calculated</h3>
                <span class="dataset-name">${operation}</span>
            </div>
            
            <div class="data-stats">
                <div class="stat-item">
                    <div class="stat-value">${formatNumber(result)}</div>
                    <div class="stat-label">${operation}</div>
                </div>
            </div>
            
            <div class="data-section">
                <h4>Details</h4>
                <p class="column-info">Computed ${func} of column "${column}" across all ${data.row_count.toLocaleString()} rows</p>
            </div>
            
            <div class="data-actions">
                <button class="secondary-btn" onclick="clearResults()">New Query</button>
            </div>
        </div>
    `;
}
