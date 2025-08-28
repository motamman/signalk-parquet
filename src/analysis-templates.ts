import { AnalysisRequest } from './claude-analyzer';

// Analysis Template Types
export interface AnalysisTemplate {
  id: string;
  name: string;
  description: string;
  category: 'navigation' | 'environment' | 'electrical' | 'safety' | 'performance' | 'maintenance';
  requiredPaths: string[];
  optionalPaths?: string[];
  icon: string;
  analysisType: 'summary' | 'anomaly' | 'trend' | 'correlation' | 'custom';
  prompt: string;
  defaultTimeRange: '1h' | '6h' | '24h' | '7d' | '30d';
  complexity: 'simple' | 'moderate' | 'complex';
  estimatedTime: string; // e.g., "30 seconds", "2 minutes"
}

export interface TemplateCategory {
  id: string;
  name: string;
  description: string;
  icon: string;
  templates: AnalysisTemplate[];
}

// Pre-built Analysis Templates for Maritime Operations
export const ANALYSIS_TEMPLATES: Record<string, AnalysisTemplate> = {
  // Navigation Templates
  'navigation-summary': {
    id: 'navigation-summary',
    name: 'Navigation Summary',
    description: 'Comprehensive analysis of recent navigation patterns and route efficiency',
    category: 'navigation',
    requiredPaths: ['navigation.position'],
    optionalPaths: ['navigation.courseOverGround', 'navigation.speedOverGround', 'navigation.headingTrue'],
    icon: '🧭',
    analysisType: 'summary',
    prompt: `Analyze this vessel's navigation data and provide insights on:
1. Route patterns and efficiency
2. Speed and course variations
3. Distance traveled and time at sea
4. Anchoring or stationary periods
5. Navigation safety observations
6. Fuel efficiency implications based on speed patterns

Focus on practical operational insights for the vessel operator.`,
    defaultTimeRange: '24h',
    complexity: 'moderate',
    estimatedTime: '45 seconds'
  },

  'route-optimization': {
    id: 'route-optimization',
    name: 'Route Optimization',
    description: 'Identify opportunities to optimize routes for efficiency and safety',
    category: 'navigation',
    requiredPaths: ['navigation.position', 'navigation.courseOverGround'],
    optionalPaths: ['navigation.speedOverGround', 'environment.wind.speedTrue'],
    icon: '📍',
    analysisType: 'custom',
    prompt: `Analyze the vessel's route data to identify optimization opportunities:
1. Route efficiency - identify unnecessary detours or inefficient paths
2. Speed optimization - analyze speed variations and their impact
3. Course corrections - examine frequent course changes
4. Weather routing considerations if wind data is available
5. Time-based patterns - identify optimal travel times
6. Safety considerations - evaluate route safety aspects

Provide specific, actionable recommendations for future route planning.`,
    defaultTimeRange: '7d',
    complexity: 'complex',
    estimatedTime: '90 seconds'
  },

  'anchoring-analysis': {
    id: 'anchoring-analysis',
    name: 'Anchoring Behavior Analysis',
    description: 'Analyze anchoring patterns, duration, and safety considerations',
    category: 'navigation',
    requiredPaths: ['navigation.position'],
    optionalPaths: ['navigation.state', 'environment.depth.belowKeel'],
    icon: '⚓',
    analysisType: 'custom',
    prompt: `Examine the vessel's anchoring behavior and patterns:
1. Anchoring locations and frequency
2. Duration of anchoring periods
3. Anchor drag detection (position drift while anchored)
4. Anchoring in relation to depth (if available)
5. Anchoring safety patterns
6. Seasonal or time-based anchoring preferences

Provide insights on anchoring safety and best practices.`,
    defaultTimeRange: '30d',
    complexity: 'moderate',
    estimatedTime: '60 seconds'
  },

  // Environment Templates
  'weather-impact': {
    id: 'weather-impact',
    name: 'Weather Impact Analysis',
    description: 'Analyze how weather conditions affect vessel performance and operations',
    category: 'environment',
    requiredPaths: ['environment.wind.speedTrue'],
    optionalPaths: ['environment.wind.directionTrue', 'navigation.speedOverGround', 'environment.outside.pressure'],
    icon: '🌊',
    analysisType: 'correlation',
    prompt: `Analyze the relationship between weather conditions and vessel performance:
1. Wind impact on vessel speed and course
2. Weather patterns and their operational effects
3. Optimal weather windows for different activities
4. Weather-related performance variations
5. Safety considerations in adverse weather
6. Seasonal weather patterns and their implications

Provide practical guidance for weather-based operational planning.`,
    defaultTimeRange: '7d',
    complexity: 'complex',
    estimatedTime: '75 seconds'
  },

  'wind-patterns': {
    id: 'wind-patterns',
    name: 'Wind Pattern Analysis',
    description: 'Detailed analysis of wind conditions and sailing performance',
    category: 'environment',
    requiredPaths: ['environment.wind.speedTrue', 'environment.wind.directionTrue'],
    optionalPaths: ['environment.wind.speedApparent', 'navigation.speedOverGround'],
    icon: '💨',
    analysisType: 'trend',
    prompt: `Provide detailed wind analysis for sailing optimization:
1. Wind speed and direction patterns over time
2. Diurnal wind variations (daily patterns)
3. True vs apparent wind analysis
4. Optimal wind conditions for best performance
5. Wind shift patterns and their timing
6. Sailing performance in different wind conditions

Focus on actionable insights for sailing strategy and route planning.`,
    defaultTimeRange: '7d',
    complexity: 'moderate',
    estimatedTime: '60 seconds'
  },

  // Electrical Templates
  'battery-health': {
    id: 'battery-health',
    name: 'Battery Health Assessment',
    description: 'Comprehensive analysis of battery performance and charging patterns',
    category: 'electrical',
    requiredPaths: ['electrical.batteries.*.voltage'],
    optionalPaths: ['electrical.batteries.*.current', 'electrical.batteries.*.temperature'],
    icon: '🔋',
    analysisType: 'trend',
    prompt: `Analyze battery system health and performance:
1. Voltage trends and patterns
2. Charging and discharging cycles
3. Battery capacity degradation indicators
4. Temperature effects on battery performance (if available)
5. Charging system efficiency
6. Battery maintenance recommendations
7. Potential issues or warning signs

Provide maintenance schedule recommendations and battery replacement guidance.`,
    defaultTimeRange: '30d',
    complexity: 'moderate',
    estimatedTime: '60 seconds'
  },

  'power-consumption': {
    id: 'power-consumption',
    name: 'Power Consumption Analysis',
    description: 'Analyze electrical power usage patterns and efficiency',
    category: 'electrical',
    requiredPaths: ['electrical.batteries.*.current'],
    optionalPaths: ['electrical.batteries.*.voltage', 'electrical.batteries.*.power'],
    icon: '⚡',
    analysisType: 'summary',
    prompt: `Analyze electrical power consumption patterns:
1. Power usage trends over time
2. Peak consumption periods and their causes
3. Energy efficiency patterns
4. Load distribution across systems
5. Power generation vs consumption balance
6. Opportunities for energy conservation
7. System optimization recommendations

Provide practical advice for improving energy efficiency and battery life.`,
    defaultTimeRange: '7d',
    complexity: 'moderate',
    estimatedTime: '45 seconds'
  },

  // Safety Templates
  'safety-anomalies': {
    id: 'safety-anomalies',
    name: 'Safety Anomaly Detection',
    description: 'Detect unusual patterns that might indicate safety concerns',
    category: 'safety',
    requiredPaths: ['*'], // Can work with any data
    optionalPaths: [],
    icon: '⚠️',
    analysisType: 'anomaly',
    prompt: `Examine all available data for potential safety concerns:
1. Unusual operational patterns that might indicate equipment issues
2. Navigation anomalies suggesting possible emergencies
3. Electrical system irregularities that could pose risks
4. Environmental data suggesting dangerous conditions
5. Communication or data gaps that might indicate problems
6. Any patterns suggesting crew or vessel distress

Priority focus on immediate safety concerns requiring attention.`,
    defaultTimeRange: '24h',
    complexity: 'complex',
    estimatedTime: '90 seconds'
  },

  'equipment-monitoring': {
    id: 'equipment-monitoring',
    name: 'Equipment Health Monitoring',
    description: 'Monitor equipment performance and predict maintenance needs',
    category: 'maintenance',
    requiredPaths: ['*'], // Can work with various equipment data
    optionalPaths: [],
    icon: '🔧',
    analysisType: 'trend',
    prompt: `Analyze equipment performance data for maintenance insights:
1. Performance trends indicating wear or degradation
2. Operational efficiency changes over time
3. Unusual vibrations, temperatures, or other indicators
4. Predictive maintenance recommendations
5. Equipment failure risk assessment
6. Optimal maintenance scheduling suggestions
7. Cost-benefit analysis of repairs vs replacement

Focus on preventing failures and optimizing maintenance schedules.`,
    defaultTimeRange: '30d',
    complexity: 'complex',
    estimatedTime: '75 seconds'
  },

  // Performance Templates
  'fuel-efficiency': {
    id: 'fuel-efficiency',
    name: 'Fuel Efficiency Analysis',
    description: 'Analyze fuel consumption patterns and identify efficiency opportunities',
    category: 'performance',
    requiredPaths: ['navigation.speedOverGround'],
    optionalPaths: ['propulsion.*.fuel.rate', 'environment.wind.speedTrue', 'navigation.courseOverGround'],
    icon: '⛽',
    analysisType: 'correlation',
    prompt: `Analyze vessel fuel efficiency and consumption patterns:
1. Speed vs fuel consumption relationships
2. Optimal cruising speeds for efficiency
3. Weather impact on fuel consumption
4. Course and routing efficiency effects
5. Engine performance optimization opportunities
6. Fuel consumption benchmarking
7. Cost-saving recommendations

Provide specific recommendations for reducing fuel costs while maintaining operational efficiency.`,
    defaultTimeRange: '30d',
    complexity: 'complex',
    estimatedTime: '90 seconds'
  },

  'performance-trends': {
    id: 'performance-trends',
    name: 'Overall Performance Trends',
    description: 'Comprehensive analysis of vessel performance over time',
    category: 'performance',
    requiredPaths: ['navigation.speedOverGround'],
    optionalPaths: ['navigation.position', 'electrical.batteries.*.voltage', 'environment.wind.speedTrue'],
    icon: '📊',
    analysisType: 'trend',
    prompt: `Analyze overall vessel performance trends:
1. Speed and efficiency trends over time
2. Operational pattern changes and their impacts
3. Seasonal performance variations
4. System performance degradation indicators
5. Comparative performance analysis
6. Performance optimization opportunities
7. Long-term maintenance implications

Provide insights for improving overall vessel performance and operational efficiency.`,
    defaultTimeRange: '30d',
    complexity: 'complex',
    estimatedTime: '90 seconds'
  }
};

// Organized template categories for UI display
export const TEMPLATE_CATEGORIES: TemplateCategory[] = [
  {
    id: 'navigation',
    name: 'Navigation & Routing',
    description: 'Route analysis, navigation patterns, and anchoring behavior',
    icon: '🧭',
    templates: [
      ANALYSIS_TEMPLATES['navigation-summary'],
      ANALYSIS_TEMPLATES['route-optimization'],
      ANALYSIS_TEMPLATES['anchoring-analysis']
    ]
  },
  {
    id: 'environment',
    name: 'Weather & Environment',
    description: 'Weather impact analysis and environmental conditions',
    icon: '🌊',
    templates: [
      ANALYSIS_TEMPLATES['weather-impact'],
      ANALYSIS_TEMPLATES['wind-patterns']
    ]
  },
  {
    id: 'electrical',
    name: 'Electrical Systems',
    description: 'Battery health, power consumption, and electrical system analysis',
    icon: '🔋',
    templates: [
      ANALYSIS_TEMPLATES['battery-health'],
      ANALYSIS_TEMPLATES['power-consumption']
    ]
  },
  {
    id: 'safety',
    name: 'Safety & Monitoring',
    description: 'Safety anomaly detection and equipment health monitoring',
    icon: '⚠️',
    templates: [
      ANALYSIS_TEMPLATES['safety-anomalies'],
      ANALYSIS_TEMPLATES['equipment-monitoring']
    ]
  },
  {
    id: 'performance',
    name: 'Performance & Efficiency',
    description: 'Fuel efficiency and overall performance optimization',
    icon: '📊',
    templates: [
      ANALYSIS_TEMPLATES['fuel-efficiency'],
      ANALYSIS_TEMPLATES['performance-trends']
    ]
  }
];

// Template Management Class
export class AnalysisTemplateManager {
  /**
   * Get all available templates
   */
  static getAllTemplates(): AnalysisTemplate[] {
    return Object.values(ANALYSIS_TEMPLATES);
  }

  /**
   * Get templates by category
   */
  static getTemplatesByCategory(category: string): AnalysisTemplate[] {
    return Object.values(ANALYSIS_TEMPLATES).filter(template => template.category === category);
  }

  /**
   * Get template by ID
   */
  static getTemplate(templateId: string): AnalysisTemplate | null {
    return ANALYSIS_TEMPLATES[templateId] || null;
  }

  /**
   * Get template categories for UI
   */
  static getTemplateCategories(): TemplateCategory[] {
    return TEMPLATE_CATEGORIES;
  }

  /**
   * Find templates that can work with given data paths
   */
  static findCompatibleTemplates(availablePaths: string[]): AnalysisTemplate[] {
    return Object.values(ANALYSIS_TEMPLATES).filter(template => {
      // Check if all required paths are available
      return template.requiredPaths.every(requiredPath => {
        if (requiredPath === '*') return true; // Wildcard matches anything
        if (requiredPath.includes('*')) {
          // Handle wildcard paths like "electrical.batteries.*"
          const pattern = requiredPath.replace(/\*/g, '.*');
          const regex = new RegExp(pattern);
          return availablePaths.some(path => regex.test(path));
        }
        return availablePaths.includes(requiredPath);
      });
    });
  }

  /**
   * Create analysis request from template
   */
  static createAnalysisRequest(
    templateId: string, 
    dataPath: string,
    customPrompt?: string,
    timeRange?: { start: Date; end: Date }
  ): AnalysisRequest | null {
    const template = this.getTemplate(templateId);
    if (!template) return null;

    const request: AnalysisRequest = {
      dataPath,
      analysisType: template.analysisType,
      customPrompt: customPrompt || template.prompt,
      timeRange: timeRange || this.getDefaultTimeRange(template.defaultTimeRange),
      context: {
        templateId: template.id,
        templateName: template.name,
        category: template.category
      }
    };

    return request;
  }

  /**
   * Convert default time range string to actual dates
   */
  private static getDefaultTimeRange(timeRange: string): { start: Date; end: Date } {
    const end = new Date();
    const start = new Date();

    switch (timeRange) {
      case '1h':
        start.setHours(end.getHours() - 1);
        break;
      case '6h':
        start.setHours(end.getHours() - 6);
        break;
      case '24h':
        start.setDate(end.getDate() - 1);
        break;
      case '7d':
        start.setDate(end.getDate() - 7);
        break;
      case '30d':
        start.setDate(end.getDate() - 30);
        break;
      default:
        start.setDate(end.getDate() - 1); // Default to 24h
    }

    return { start, end };
  }

  /**
   * Get template suggestions based on data path
   */
  static getTemplateSuggestions(dataPath: string): AnalysisTemplate[] {
    const suggestions: AnalysisTemplate[] = [];
    
    // Navigation path suggestions
    if (dataPath.includes('navigation.position')) {
      suggestions.push(
        ANALYSIS_TEMPLATES['navigation-summary'],
        ANALYSIS_TEMPLATES['route-optimization'],
        ANALYSIS_TEMPLATES['anchoring-analysis']
      );
    }
    
    // Wind data suggestions
    if (dataPath.includes('environment.wind')) {
      suggestions.push(
        ANALYSIS_TEMPLATES['weather-impact'],
        ANALYSIS_TEMPLATES['wind-patterns'],
        ANALYSIS_TEMPLATES['fuel-efficiency']
      );
    }
    
    // Battery data suggestions
    if (dataPath.includes('electrical.batteries')) {
      suggestions.push(
        ANALYSIS_TEMPLATES['battery-health'],
        ANALYSIS_TEMPLATES['power-consumption']
      );
    }
    
    // Speed data suggestions
    if (dataPath.includes('speedOverGround') || dataPath.includes('speedThroughWater')) {
      suggestions.push(
        ANALYSIS_TEMPLATES['performance-trends'],
        ANALYSIS_TEMPLATES['fuel-efficiency']
      );
    }
    
    // Always include safety monitoring for any data
    suggestions.push(ANALYSIS_TEMPLATES['safety-anomalies']);
    
    // Remove duplicates
    return suggestions.filter((template, index, self) => 
      index === self.findIndex(t => t.id === template.id)
    );
  }

  /**
   * Validate template configuration
   */
  static validateTemplate(template: AnalysisTemplate): { valid: boolean; errors: string[] } {
    const errors: string[] = [];

    if (!template.id || template.id.trim() === '') {
      errors.push('Template ID is required');
    }

    if (!template.name || template.name.trim() === '') {
      errors.push('Template name is required');
    }

    if (!template.prompt || template.prompt.trim() === '') {
      errors.push('Template prompt is required');
    }

    if (!template.requiredPaths || template.requiredPaths.length === 0) {
      errors.push('At least one required path must be specified');
    }

    if (!['simple', 'moderate', 'complex'].includes(template.complexity)) {
      errors.push('Template complexity must be simple, moderate, or complex');
    }

    if (!['navigation', 'environment', 'electrical', 'safety', 'performance', 'maintenance'].includes(template.category)) {
      errors.push('Template category must be valid');
    }

    return {
      valid: errors.length === 0,
      errors
    };
  }
}