import * as PathBrowser from './pathBrowser.js';
import * as DataPaths from './dataPaths.js';
import * as DataValidation from './dataValidation.js';
import * as CloudStatus from './cloudStatus.js';
import * as PathConfig from './pathConfig.js';
import * as CommandManager from './commandManager.js';
import * as LiveConnections from './liveConnections.js';
import * as Analysis from './analysis.js';

function registerGlobals(mapping) {
    Object.entries(mapping).forEach(([name, fn]) => {
        window[name] = fn;
    });
}

function showTab(tabId) {
    const panels = document.querySelectorAll('.tab-panel');
    panels.forEach(panel => panel.classList.remove('active'));

    const selectedPanel = document.getElementById(tabId);
    if (selectedPanel) {
        selectedPanel.classList.add('active');
    }

    const buttons = document.querySelectorAll('.tab-button');
    buttons.forEach(button => button.classList.remove('active'));

    const activeButton = document.querySelector(`.tab-button[onclick="showTab('${tabId}')"]`);
    if (activeButton) {
        activeButton.classList.add('active');
    }

    if (tabId === 'aiAnalysis') {
        Analysis.initializeAIAnalysisTab();
    }

    if (tabId === 'commandManager') {
        CommandManager.loadCommands();
    }

    if (tabId === 'liveConnections') {
        LiveConnections.loadStreams();
    }
}

registerGlobals({
    showTab,
    // Path browser & queries
    loadAvailablePaths: PathBrowser.loadAvailablePaths,
    updateSelectedPath: PathBrowser.updateSelectedPath,
    generateQueryForSelectedPath: PathBrowser.generateQueryForSelectedPath,
    analyzeSelectedPath: PathBrowser.analyzeSelectedPath,
    generateExampleQueries: PathBrowser.generateExampleQueries,
    executeQuery: DataPaths.executeQuery,
    executeDataPathsQuery: DataPaths.executeDataPathsQuery,
    setQuery: DataPaths.setQuery,
    setDataPathsQuery: DataPaths.setDataPathsQuery,
    clearQuery: DataPaths.clearQuery,
    clearDataPathsQuery: DataPaths.clearDataPathsQuery,
    // Data validation
    runDataValidation: DataValidation.runDataValidation,
    cancelValidation: DataValidation.cancelValidation,
    repairSchemas: DataValidation.repairSchemas,
    cancelRepair: DataValidation.cancelRepair,
    // Cloud status
    testS3Connection: CloudStatus.testS3Connection,
    // Path configuration
    loadPathConfigurations: PathConfig.loadPathConfigurations,
    toggleCommandPaths: PathConfig.toggleCommandPaths,
    showAddPathForm: PathConfig.showAddPathForm,
    hideAddPathForm: PathConfig.hideAddPathForm,
    addCustomRegimen: PathConfig.addCustomRegimen,
    addPathConfiguration: PathConfig.addPathConfiguration,
    removePathConfiguration: PathConfig.removePathConfiguration,
    editPathConfiguration: PathConfig.editPathConfiguration,
    updatePathFilter: PathConfig.updatePathFilter,
    updateEditPathFilter: PathConfig.updateEditPathFilter,
    saveEdit: PathConfig.saveEdit,
    cancelEdit: PathConfig.cancelEdit,
    // Command management
    showAddCommandForm: CommandManager.showAddCommandForm,
    hideAddCommandForm: CommandManager.hideAddCommandForm,
    showEditCommandForm: CommandManager.showEditCommandForm,
    hideEditCommandForm: CommandManager.hideEditCommandForm,
    updateCommand: CommandManager.updateCommand,
    registerCommand: CommandManager.registerCommand,
    executeCommand: CommandManager.executeCommand,
    toggleCommand: CommandManager.toggleCommand,
    unregisterCommand: CommandManager.unregisterCommand,
    updateThresholdPathFilter: CommandManager.updateThresholdPathFilter,
    toggleNewThresholdValueField: CommandManager.toggleNewThresholdValueField,
    addNewThreshold: CommandManager.addNewThreshold,
    cancelNewThreshold: CommandManager.cancelNewThreshold,
    saveNewThreshold: CommandManager.saveNewThreshold,
    removeThreshold: CommandManager.removeThreshold,
    updateAddCmdThresholdPathFilter: CommandManager.updateAddCmdThresholdPathFilter,
    toggleAddCmdThresholdValueField: CommandManager.toggleAddCmdThresholdValueField,
    addNewCommandThreshold: CommandManager.addNewCommandThreshold,
    cancelAddCmdThreshold: CommandManager.cancelAddCmdThreshold,
    saveAddCmdThreshold: CommandManager.saveAddCmdThreshold,
    removeAddCommandThreshold: CommandManager.removeAddCommandThreshold,
    setManualOverride: CommandManager.setManualOverride,
    promptManualOverride: CommandManager.promptManualOverride,
    loadCommandHistory: CommandManager.loadCommandHistory,
    toggleAutomation: CommandManager.toggleAutomation,
    updateAllAutomationStates: CommandManager.updateAllAutomationStates,
    // Live connections
    loadStreams: LiveConnections.loadStreams,
    showAddStreamForm: LiveConnections.showAddStreamForm,
    hideAddStreamForm: LiveConnections.hideAddStreamForm,
    refreshSignalKPaths: LiveConnections.refreshSignalKPaths,
    createStream: LiveConnections.createStream,
    startStream: LiveConnections.startStream,
    pauseStream: LiveConnections.pauseStream,
    stopStream: LiveConnections.stopStream,
    deleteStream: LiveConnections.deleteStream,
    editStream: LiveConnections.editStream,
    updateStream: LiveConnections.updateStream,
    cancelEditStream: LiveConnections.cancelEditStream,
    clearLiveData: LiveConnections.clearLiveData,
    toggleLiveDataPause: LiveConnections.toggleLiveDataPause,
    showDataSummary: LiveConnections.showDataSummary,
    // Analysis & AI
    testClaudeConnection: Analysis.testClaudeConnection,
    loadVesselContext: Analysis.loadVesselContext,
    toggleVesselContext: Analysis.toggleVesselContext,
    refreshVesselInfo: Analysis.refreshVesselInfo,
    saveVesselContext: Analysis.saveVesselContext,
    previewClaudeContext: Analysis.previewClaudeContext,
    closeContextPreview: Analysis.closeContextPreview,
    loadAnalysisTemplates: Analysis.loadAnalysisTemplates,
    runQuickAnalysis: Analysis.runQuickAnalysis,
    getSelectedDataPaths: Analysis.getSelectedDataPaths,
    updateSelectedPathCount: Analysis.updateSelectedPathCount,
    handlePathCheckboxChange: Analysis.handlePathCheckboxChange,
    selectAllPaths: Analysis.selectAllPaths,
    clearAllPaths: Analysis.clearAllPaths,
    populateAnalysisPathCheckboxes: Analysis.populateAnalysisPathCheckboxes,
    cancelAnalysis: Analysis.cancelAnalysis,
    runCustomAnalysis: Analysis.runCustomAnalysis,
    openAnalysisHistoryModal: Analysis.openAnalysisHistoryModal,
    closeAnalysisHistoryModal: Analysis.closeAnalysisHistoryModal,
    viewAnalysis: Analysis.viewAnalysis,
    deleteAnalysis: Analysis.deleteAnalysis,
    closeAnalysisViewModal: Analysis.closeAnalysisViewModal,
    analyzeDataPath: Analysis.analyzeDataPath,
    toggleAnalysisMode: Analysis.toggleAnalysisMode,
    toggleSelectionOptions: Analysis.toggleSelectionOptions,
    loadAvailableDataPaths: Analysis.loadAvailableDataPaths,
    loadDataPathsForAnalysis: Analysis.loadDataPathsForAnalysis,
    askFollowUpQuestion: Analysis.askFollowUpQuestion
});

document.addEventListener('DOMContentLoaded', async () => {
    await PathBrowser.loadAvailablePaths();
    await PathConfig.loadPathConfigurations();
    PathBrowser.generateExampleQueries();
    PathConfig.initPathConfigListeners();
    await LiveConnections.initLiveConnections();
});
