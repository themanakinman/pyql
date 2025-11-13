// ========================================
// SELECTION ACTION - Column Projection
// ========================================

// Validate selection input
validateSelectionInput = function(input) {
    if (!loadedDataFrame) {
        return { valid: false, error: 'Please load data first' };
    }
    
    const selectionRegex = /^(\w+)\[([^\]]+)\]$/;
    const match = input.match(selectionRegex);
    
    if (!match) {
        return { 
            valid: false, 
            error: 'Invalid syntax. Use: file_name[Column1, Column2, Column3]' 
        };
    }
    
    // Parse columns
    const [, dfName, columnsStr] = match;  // Destructure the match array
    const columns = columnsStr.split(',').map(c => c.trim());
    
    // Validate all columns exist
    for (const col of columns) {
        if (!loadedDataFrame.columns.includes(col)) {
            return { 
                valid: false, 
                error: `Column "${col}" not found. Available: ${loadedDataFrame.columns.join(', ')}` 
            };
        }
    }
    
    if (columns.length === 0) {
        return { valid: false, error: 'Please specify at least one column' };
    }
    
     return { 
        valid: true,
        dfName,
        columns
    };
};


// Execute selection
executeSelection = async function(input) {
    showLoading('Selecting columns...');

    const validation = validateSelectionInput(input);
    if (!validation.valid) {
        showError(validation.error);
        return;
    }
    
    const { dfName, columns } = validation
    try {
        const response = await fetch('/api/select', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                dataframe: dfName,
                columns: columns
            })
        });
        
        const data = await response.json();
        
        if (!response.ok) {
            showError(data.error || 'Failed to select columns');
            return;
        }
        
        displaySelectionResults(data, columns);
        
    } catch (error) {
        showError('Error selecting columns: ' + error.message);
    }
};

// Display selection results
function displaySelectionResults(data, columns) {
    const resultsContainer = document.getElementById('results');
    const tableHTML = createDataTable(data.data);
    
    const columnList = columns.map(col => 
        `<span class="column-badge">${escapeHtml(col)}</span>`
    ).join('');
    
    resultsContainer.innerHTML = `
        <div class="data-card">
            <div class="data-header">
                <h3>✓ Columns Selected</h3>
                <span class="dataset-name">${data.rows.toLocaleString()} rows</span>
            </div>
            
            <div class="data-section">
                <h4>Selected Columns</h4>
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
