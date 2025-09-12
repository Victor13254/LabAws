// Variables globales
let currentData = null;

// Elementos DOM
const elements = {
    form: document.getElementById('queryForm'),
    startDate: document.getElementById('startDate'),
    endDate: document.getElementById('endDate'),
    limit: document.getElementById('limit'),
    queryBtn: document.getElementById('queryBtn'),
    status: document.getElementById('status'),
    results: document.getElementById('results'),
    tableContainer: document.getElementById('tableContainer'),
    downloadBtn: document.getElementById('downloadBtn')
};

// Configurar fechas por defecto (últimos 2 días)
function setDefaultDates() {
    const now = new Date();
    const twoDaysAgo = new Date(now.getTime() - 2 * 24 * 60 * 60 * 1000);
    
    // Formato para datetime-local: YYYY-MM-DDTHH:mm
    elements.startDate.value = twoDaysAgo.toLocaleString('sv-SE').slice(0, 16);
    elements.endDate.value   = now.toLocaleString('sv-SE').slice(0, 16);

}

// Utilidades para mostrar estado
const statusUtils = {
    show: (message, type) => {
        elements.status.textContent = message;
        elements.status.className = `status ${type}`;
        elements.status.style.display = 'block';
    },
    
    hide: () => {
        elements.status.style.display = 'none';
    }
};

// Crear tabla con los resultados
function createTable(data) {
    if (!data || data.length === 0) {
        return '<div class="no-data">No se encontraron registros en el rango seleccionado</div>';
    }
    
    let html = `
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>Fecha</th>
                    <th>Valor</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    data.forEach((item, index) => {
        const fecha = new Date(item.fecha).toLocaleString('es-ES');
        const valor = Number(item.valor).toFixed(6);
        
        html += `
            <tr>
                <td>${index + 1}</td>
                <td>${fecha}</td>
                <td>$${valor}</td>
            </tr>
        `;
    });
    
    html += `
            </tbody>
        </table>
    `;
    
    return html;
}

// Validar formulario
function validateForm() {
    if (!elements.startDate.value || !elements.endDate.value) {
        statusUtils.show('Por favor selecciona ambas fechas', 'error');
        return false;
    }
    
    if (new Date(elements.endDate.value) <= new Date(elements.startDate.value)) {
        statusUtils.show('La fecha fin debe ser mayor que la fecha inicio', 'error');
        return false;
    }
    
    return true;
}

// Descargar JSON
function downloadJSON() {
    if (!currentData) return;
    
    const dataStr = JSON.stringify(currentData, null, 2);
    const dataBlob = new Blob([dataStr], { type: 'application/json' });
    
    const link = document.createElement('a');
    link.href = URL.createObjectURL(dataBlob);
    link.download = `datos_${new Date().toLocaleDateString('sv-SE')}.json`;
    link.click();
    
    URL.revokeObjectURL(link.href);
}

// Realizar consulta
async function performQuery(event) {
    event.preventDefault();
    
    // Validar formulario
    if (!validateForm()) {
        return;
    }
    
    // Preparar datos
    const payload = {
    start: elements.startDate.value + ":00", 
    end:   elements.endDate.value + ":00",
    limit: parseInt(elements.limit.value) || 1000
    };

    
    // Mostrar estado de carga
    elements.queryBtn.disabled = true;
    statusUtils.show('Consultando base de datos...', 'loading');
    elements.results.style.display = 'none';
    
    try {
        // Realizar petición
        const response = await fetch('/valores/rango', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload)
        });
        
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Error ${response.status}: ${errorText}`);
        }
        
        const data = await response.json();
        currentData = data;
        
        // Mostrar resultados
        elements.tableContainer.innerHTML = createTable(data.items);
        statusUtils.show(`✅ Consulta exitosa: ${data.count} registros encontrados`, 'success');
        elements.results.style.display = 'block';
        
        // Mostrar botón de descarga si hay datos
        if (data.items && data.items.length > 0) {
            elements.downloadBtn.style.display = 'inline-block';
        } else {
            elements.downloadBtn.style.display = 'none';
        }
        
    } catch (error) {
        console.error('Error en la consulta:', error);
        statusUtils.show(`❌ Error: ${error.message}`, 'error');
        elements.results.style.display = 'none';
        currentData = null;
    } finally {
        elements.queryBtn.disabled = false;
    }
}

// Configurar event listeners
function setupEventListeners() {
    elements.form.addEventListener('submit', performQuery);
    elements.downloadBtn.addEventListener('click', downloadJSON);
}

// Inicialización de la aplicación
function init() {
    setDefaultDates();
    setupEventListeners();
    
    // Opcional: realizar consulta automática al cargar
    // setTimeout(() => elements.form.dispatchEvent(new Event('submit')), 1000);
}

// Iniciar cuando el DOM esté listo
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}