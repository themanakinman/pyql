// ========================================
// STATE MANAGEMENT
// ========================================
let currentAction = 'load';
let loadedDataFrame = null;

console.log('🚀 PyQL main.js loaded');

// Action configurations (will be populated after all scripts load)
let ACTIONS = {};

// ========================================
// INITIALIZATION
// ========================================
document.addEventListener('DOMContentLoaded', function() {
    ACTIONS = {
        load: {
            placeholder: 'data/rappers.csv',
            description: 'Load a CSV file',
            validator: validateLoadInput,
            executor: executeLoad
        },
        filter: {
            placeholder: 'df[artist] == Mobb Deep & df[id] > 3',
            description: 'Filter rows by condition',
            validator: validateFilterInput,
            executor: executeFilter
        },
        select: {
            placeholder: 'df[title, artist, points]',
            description: 'Select specific columns',
            validator: validateSelectionInput,
            executor: executeSelection
        },
        aggregate: {
            placeholder: 'df.sum(points)',
            description: 'Calculate aggregate (sum, mean, max, etc.)',
            validator: validateAggregateInput,
            executor: executeAggregate
        },
        groupby: {
            placeholder: 'df.groupby(artist).sum(points)',
            description: 'Group and aggregate data',
            validator: validateGroupByInput,
            executor: executeGroupBy
        },
        join: {
            placeholder: 'df1.merge(df2, on=CountryCode)',
            description: 'Join two dataframes',
            validator: validateJoinInput,
            executor: executeJoin
        }
    };
    
    selectAction('load');
    
    const input = document.getElementById('query-input');
    if (input) {
        input.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') executeQuery();
        });
    }
});

// ========================================
// ACTION SELECTION
// ========================================
function selectAction(action) {
    console.log(`🎯 Selecting action: ${action}`);
    
    if (!ACTIONS[action]) {
        console.error(`❌ Unknown action: ${action}`);
        return;
    }
    
    currentAction = action;
    
    // Update button styles
    document.querySelectorAll('.action-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    const activeBtn = document.querySelector(`[onclick="selectAction('${action}')"]`);
    if (activeBtn) {
        activeBtn.classList.add('active');
        console.log(`✅ Activated ${action} button`);
    }
    
    // Update input placeholder
    const input = document.getElementById('query-input');
    if (input) {
        input.placeholder = ACTIONS[action].placeholder;
        input.value = '';
        input.focus();
    }
}

// ========================================
// QUERY EXECUTION ROUTER
// ========================================
async function executeQuery() {
    console.log('🚀 executeQuery() called');
    console.log(`Current action: ${currentAction}`);
    
    const input = document.getElementById('query-input');
    if (!input) {
        console.error('❌ Input element not found');
        return;
    }
    
    const inputValue = input.value.trim();
    console.log(`Input value: "${inputValue}"`);
    
    if (!inputValue) {
        showError('Please enter a command');
        return;
    }
    
    const actionConfig = ACTIONS[currentAction];
    console.log('Action config:', actionConfig);
    console.log('Executor function:', actionConfig.executor);
    
    // Validate input
    if (actionConfig.validator) {
        console.log('Validating input...');
        const validation = actionConfig.validator(inputValue);
        console.log('Validation result:', validation);
        
        if (!validation.valid) {
            showError(validation.error);
            return;
        }
    }
    
    // Execute action
    if (actionConfig.executor) {
        console.log(`Executing ${currentAction}...`);
        await actionConfig.executor(inputValue);
    } else {
        showError(`${currentAction} is not yet implemented`);
    }
}

// ========================================
// UI HELPERS
// ========================================
function showLoading(message = 'Processing...') {
    console.log(`⏳ showLoading: ${message}`);
    
    const resultsContainer = document.getElementById('results');
    if (!resultsContainer) {
        console.error('❌ Results container not found');
        return;
    }
    
    resultsContainer.classList.add('show');
    resultsContainer.innerHTML = `
        <div class="loading">
            <div class="spinner"></div>
            <p>${message}</p>
        </div>
    `;
    
    console.log('✅ Loading UI displayed');
}

function showError(message) {
    console.log(`❌ showError: ${message}`);
    
    const resultsContainer = document.getElementById('results');
    if (!resultsContainer) {
        console.error('❌ Results container not found');
        return;
    }
    
    resultsContainer.classList.add('show');
    resultsContainer.innerHTML = `
        <div class="error-card">
            <div class="error-icon">⚠️</div>
            <h3>Error</h3>
            <p>${message}</p>
            <button class="secondary-btn" onclick="clearResults()">Try Again</button>
        </div>
    `;
    
    console.log('✅ Error UI displayed');
}

function clearResults() {
    console.log('🧹 Clearing results');
    
    const resultsContainer = document.getElementById('results');
    if (resultsContainer) {
        resultsContainer.classList.remove('show');
        resultsContainer.innerHTML = '';
    }
    
    const input = document.getElementById('query-input');
    if (input) {
        input.value = '';
        input.focus();
    }
}

async function clearData() {
    console.log('🗑️ Clearing data');
    
    try {
        const response = await fetch('/api/clear', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        });
        
        const data = await response.json();
        console.log('Clear response:', data);
        
        loadedDataFrame = null;
        clearResults();
        selectAction('load');
        
        console.log('✅ Data cleared successfully');
    } catch (error) {
        console.error('❌ Error clearing data:', error);
    }
}

// ========================================
// VALIDATION PLACEHOLDERS
// ========================================
function validateLoadInput(input) {
    console.log('Validating load input (placeholder):', input);
    return { valid: true };
}

function validateFilterInput(input) {
    console.log('Validating filter input (placeholder):', input);
    return { valid: true };
}

function validateSelectionInput(input) {
    return { valid: true };
}

function validateGroupByInput(input) {
    return { valid: true };
}

function validateAggregateInput(input) {
    return { valid: true };
}

function validateJoinInput(input) {
    return { valid: true };
}

// ========================================
// EXECUTION PLACEHOLDERS
// ========================================
async function executeLoad(input) {
    console.log('executeLoad placeholder called');
}

async function executeFilter(input) {
    console.log('executeFilter placeholder called');
}

async function executeSelection(input) {
}

async function executeGroupBy(input) {
}

async function executeAggregate(input) {

}
async function executeJoin(input) {
    
}
