// ========================================
// JOIN ACTION - Merge DataFrames
// ========================================

// Validate join input
validateJoinInput = function(input) {
    if (!loadedDataFrame) {
        return { valid: false, error: 'Please load data first' };
    }
    
    // Parse syntax: df1.merge(df2, on=Column) or df1.merge(df2, left_on=Col1, right_on=Col2)
    const joinRegex = /^(\w+)\.merge\((\w+),\s*(?:on=(\w+)|left_on=(\w+),\s*right_on=(\w+))(?:,\s*how=(\w+))?\)$/;
    const match = input.match(joinRegex);
    
    if (!match) {
        return { 
            valid: false, 
            error: 'Invalid syntax. Use: file_name1.merge(file_name2, on=Column) or file_name1.merge(file_name2, left_on=Col1, right_on=Col2)' 
        };
    }
    
    return { valid: true };
};

// Execute join
executeJoin = async function(input) {
    showLoading('Joining dataframes...');
    
    const joinRegex = /^(\w+)\.merge\((\w+),\s*(?:on=(\w+)|left_on=(\w+),\s*right_on=(\w+))(?:,\s*how=(\w+))?\)$/;
    const match = input.match(joinRegex);
    
    if (!match) {
        return { 
            valid: false, 
            error: 'Invalid syntax. Use: file_name1.merge(file_name2, on=Column) or file_name1.merge(file_name2, left_on=Col1, right_on=Col2)' 
        };
    }
    
    const [, leftDf, rightDf, onCol, leftOn, rightOn, how] = match;
    
    // Determine join columns
    const joinParams = {
        left: leftDf,
        right: rightDf,
        how: how || 'inner'
    };
    
    if (onCol) {
        // Same column name in both DataFrames
        joinParams.left_on = onCol;
        joinParams.right_on = onCol;
    } else {
        // Different column names
        joinParams.left_on = leftOn;
        joinParams.right_on = rightOn;
    }
    
    try {
        const response = await fetch('/api/join', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(joinParams)
        });
        
        const data = await response.json();
        
        if (!response.ok) {
            showError(data.error || 'Failed to join dataframes');
            return;
        }
        
        displayJoinResults(data, input);
        
    } catch (error) {
        showError('Error joining dataframes: ' + error.message);
    }
};

// Display join results
function displayJoinResults(data, query) {
    const resultsContainer = document.getElementById('results');
    const tableHTML = createDataTable(data.data);
    
    const columnList = data.columns.slice(0, 10).map(col => 
        `<span class="column-badge">${escapeHtml(col)}</span>`
    ).join('');
    
    const totalCols = data.columns.length;
    const displayedCols = Math.min(10, totalCols);
    const columnNote = totalCols > 10
        ? `<p class="column-info">Displaying a max of ${displayedCols} out of ${totalCols} columns for performance*</p>`
        : '';
    
    resultsContainer.innerHTML = `
        <div class="data-card">
            <div class="data-header">
                <h3>✓ DataFrames Joined</h3>
                <span class="dataset-name">${data.rows.toLocaleString()} rows</span>
            </div>
            
            <div class="data-section">
                <h4>Query</h4>
                <code class="query-display">${escapeHtml(query)}</code>
            </div>
            
            <div class="data-section">
                <h4>Result Columns</h4>
                ${columnNote}
                <div class="column-list">${columnList}</div>
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
