// ========================================
// LOAD ACTION - CSV File Loading
// ========================================

// Override validation from main.js
validateLoadInput = function(filepath) {
    if (!filepath || filepath.trim() === '') {
        return { valid: false, error: 'Please enter a file path' };
    }
    
    if (!filepath.toLowerCase().endsWith('.csv')) {
        return { valid: false, error: 'File must be a CSV (.csv extension required)' };
    }
    
    return { valid: true };
};

// Override execution from main.js
executeLoad = async function(filepath) {
    showLoading('Loading your data...');
    
    try {
        const response = await fetch('/api/load', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                filepath: filepath,
                name: 'df'
            })
        });
        
        if (!response.ok) {
            const data = await response.json();
            showError(data.error || 'Failed to load file');
            return;
        }
        
        const data = await response.json();
        
        // Store loaded dataframe info
        loadedDataFrame = data;
        
        // Display success card
        displayDataInfo(data);
        
    } catch (error) {
        showError('Error loading file: ' + error.message);
    }
};

// Display data information card - OPTIMIZED
function displayDataInfo(data) {
    const resultsContainer = document.getElementById('results');
    if (!resultsContainer) return;
    
    const totalCols = data.total_columns || data.columns.length;
    const displayedCols = data.columns.length;
    const rowsFormatted = data.rows.toLocaleString();
    
    // Pre-build column info
    const columnInfo = totalCols > displayedCols 
        ? `<p class="column-info">Displaying a max of ${displayedCols} out of ${totalCols} columns for performance*</p>`
        : '';
    
    // Single pass through columns
    const columnBadges = data.columns.reduce((html, col) => 
        html + `<span class="column-badge">${col}</span>`, '');
    
    // Single innerHTML assignment (minimal reflow)
    resultsContainer.innerHTML = `<div class="data-card"><div class="data-header"><h3>✓ Data Loaded Successfully</h3><span class="dataset-name">${data.name}</span></div><div class="data-stats"><div class="stat-item"><div class="stat-value">${rowsFormatted}</div><div class="stat-label">Rows</div></div><div class="stat-divider"></div><div class="stat-item"><div class="stat-value">${totalCols}</div><div class="stat-label">Columns</div></div></div><div class="data-section"><h4>Columns (Preview)</h4>${columnInfo}<div class="column-list">${columnBadges}</div></div><div class="data-actions"><button class="secondary-btn" onclick="clearData()">Clear</button></div></div>`;
}
