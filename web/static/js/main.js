let currentAction = 'load';
let loadedDataFrame = null;

let ACTIONS = {};

document.addEventListener('DOMContentLoaded', function() {
    ACTIONS = {
        load: {
            placeholder: 'data/rappers.csv',
            description: 'Load a CSV file',
            validator: validateLoadInput,
            executor: executeLoad
        },
        filter: {
            placeholder: 'rappers[artist] == Mobb Deep & rappers[points] > 4',
            description: 'Filter rows by condition',
            validator: validateFilterInput,
            executor: executeFilter
        },
        select: {
            placeholder: 'rappers[title, artist, points]',
            description: 'Select specific columns',
            validator: validateSelectionInput,
            executor: executeSelection
        },
        aggregate: {
            placeholder: 'rappers.sum(points)',
            description: 'Calculate aggregate (sum, mean, max, etc.)',
            validator: validateAggregateInput,
            executor: executeAggregate
        },
        groupby: {
            placeholder: 'rappers.groupby(artist).sum(points)',
            description: 'Group and aggregate data',
            validator: validateGroupByInput,
            executor: executeGroupBy
        },
        join: {
            placeholder: 'rappers.merge(polls, on=artist)',
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

function selectAction(action) {
    if (!ACTIONS[action]) {
        return;
    }
    
    currentAction = action;
    
    // update button styles
    document.querySelectorAll('.action-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    const activeBtn = document.querySelector(`[onclick="selectAction('${action}')"]`);
    if (activeBtn) {
        activeBtn.classList.add('active');
    }
    
    // update input placeholder
    const input = document.getElementById('query-input');
    if (input) {
        input.placeholder = ACTIONS[action].placeholder;
        input.value = '';
        input.focus();
    }
}

async function executeQuery() {
    const input = document.getElementById('query-input');
    if (!input) {
        return;
    }
    
    const inputValue = input.value.trim();
    
    if (!inputValue) {
        showError('Please enter a command');
        return;
    }
    
    const actionConfig = ACTIONS[currentAction];
    
    // validate input
    if (actionConfig.validator) {
        const validation = actionConfig.validator(inputValue);
        
        if (!validation.valid) {
            showError(validation.error);
            return;
        }
    }
    
    // execute action
    if (actionConfig.executor) {
        await actionConfig.executor(inputValue);
    } else {
        showError(`${currentAction} is not yet implemented`);
    }
}

function showLoading(message = 'Processing...') {
    const resultsContainer = document.getElementById('results');
    if (!resultsContainer) {
        return;
    }
    
    resultsContainer.classList.add('show');
    resultsContainer.innerHTML = `
        <div class="loading">
            <div class="spinner"></div>
            <p>${message}</p>
        </div>
    `;
}

function showError(message) {
    const resultsContainer = document.getElementById('results');
    if (!resultsContainer) {
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
}

function clearResults() {
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
    try {
        const response = await fetch('/api/clearDataframes', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            }
        });
        
        const data = await response.json();
        
        loadedDataFrame = null;
        clearResults();
        selectAction('load');
    } catch (error) {
        // o no
    }
}

function validateLoadInput(input) {
    return { valid: true };
}

function validateFilterInput(input) {
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

async function executeLoad(input) {
}

async function executeFilter(input) {
}

async function executeSelection(input) {
}

async function executeGroupBy(input) {
}

async function executeAggregate(input) {
}

async function executeJoin(input) {
}
