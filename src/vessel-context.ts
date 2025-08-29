import { ServerAPI } from '@signalk/server-api';
import { VesselContext, VesselInfo, VesselContextExtraction } from './types';
import * as fs from 'fs-extra';
import * as path from 'path';

/**
 * Vessel Context Manager - Extracts vessel information from SignalK data
 * and manages vessel context document for Claude AI analysis
 */
export class VesselContextManager {
  private app?: ServerAPI;
  private contextFilePath: string;
  private vesselContext?: VesselContext;

  // SignalK paths to extract vessel information from
  private static readonly VESSEL_DATA_PATHS: VesselContextExtraction[] = [
    // Basic identification
    {
      path: 'name',
      signalkPath: 'name',
      displayName: 'Vessel Name',
      category: 'identification'
    },
    {
      path: 'callsign',
      signalkPath: 'communication.callsignVhf',
      displayName: 'Call Sign',
      category: 'identification'
    },
    {
      path: 'mmsi',
      signalkPath: 'mmsi',
      displayName: 'MMSI',
      category: 'identification'
    },
    
    // Physical characteristics
    {
      path: 'length',
      signalkPath: 'design.length',
      displayName: 'Length Overall (LOA)',
      unit: 'm',
      category: 'physical'
    },
    {
      path: 'beam',
      signalkPath: 'design.beam',
      displayName: 'Beam',
      unit: 'm',
      category: 'physical'
    },
    {
      path: 'draft',
      signalkPath: 'design.draft',
      displayName: 'Maximum Draft',
      unit: 'm',
      category: 'physical'
    },
    {
      path: 'height',
      signalkPath: 'design.airHeight',
      displayName: 'Air Draft/Height',
      unit: 'm',
      category: 'physical'
    },
    {
      path: 'displacement',
      signalkPath: 'design.displacement',
      displayName: 'Displacement',
      unit: 'kg',
      category: 'physical'
    },
    
    // Vessel classification
    {
      path: 'vesselType',
      signalkPath: 'design.aisShipAndCargoType',
      displayName: 'Vessel Type',
      category: 'classification'
    },
    {
      path: 'flag',
      signalkPath: 'registrations.imo.country',
      displayName: 'Flag State',
      category: 'classification'
    },
    
    // Technical specifications
    {
      path: 'grossTonnage',
      signalkPath: 'registrations.imo.grossTonnage',
      displayName: 'Gross Tonnage',
      unit: 'GT',
      category: 'technical'
    },
    {
      path: 'netTonnage',
      signalkPath: 'registrations.imo.netTonnage',
      displayName: 'Net Tonnage',
      unit: 'NT',
      category: 'technical'
    },
    {
      path: 'deadWeight',
      signalkPath: 'design.deadweight',
      displayName: 'Deadweight',
      unit: 'tonnes',
      category: 'technical'
    },
    
    // Build information
    {
      path: 'builder',
      signalkPath: 'design.constructor',
      displayName: 'Builder/Constructor',
      category: 'build'
    },
    {
      path: 'buildYear',
      signalkPath: 'design.construction.year',
      displayName: 'Build Year',
      category: 'build'
    },
    {
      path: 'hullNumber',
      signalkPath: 'registrations.other.hullNumber',
      displayName: 'Hull Number',
      category: 'build'
    },
    
    // Contact information
    {
      path: 'ownerName',
      signalkPath: 'registrations.imo.owner',
      displayName: 'Owner',
      category: 'contact'
    },
    {
      path: 'port',
      signalkPath: 'port',
      displayName: 'Port of Registry',
      category: 'contact'
    }
  ];

  constructor(app?: ServerAPI, dataDirectory?: string) {
    this.app = app;
    
    // Must have a data directory - this should be the plugin's configured directory
    if (!dataDirectory) {
      throw new Error('Data directory is required for vessel context manager');
    }
    this.contextFilePath = path.join(dataDirectory, 'vessel-context.json');
    
    this.app?.debug(`Vessel context file path: ${this.contextFilePath}`);
    
    this.loadVesselContext();
  }

  /**
   * Extract vessel information from SignalK data
   */
  async extractVesselInfo(): Promise<VesselInfo> {
    const vesselInfo: VesselInfo = {};

    if (!this.app) {
      console.debug('No SignalK app available for vessel data extraction');
      return vesselInfo;
    }

    try {
      // Get vessel context (defaults to vessels.self)
      const vesselContext = this.app.getSelfPath('') || 'vessels.self';
      
      this.app.debug(`Extracting vessel info from context: ${vesselContext}`);

      // Extract data for each defined path
      for (const extraction of VesselContextManager.VESSEL_DATA_PATHS) {
        try {
          // Get value from SignalK path
          const value = this.app.getSelfPath(extraction.signalkPath);
          
          if (value !== null && value !== undefined) {
            // Convert units and handle different data types
            let processedValue = value;
            
            // Handle specific field processing
            if (extraction.path === 'vesselType') {
              // Handle AIS ship type - could be number or object
              if (typeof value === 'number') {
                processedValue = this.convertAISShipType(value);
                vesselInfo.vesselType = processedValue;
              } else if (typeof value === 'object' && value !== null) {
                if (value.name) {
                  processedValue = value.name;
                  vesselInfo.vesselType = processedValue;
                } else if (value.id !== undefined) {
                  processedValue = this.convertAISShipType(value.id);
                  vesselInfo.vesselType = processedValue;
                }
              } else if (typeof value === 'string') {
                vesselInfo.vesselType = value;
              }
            } else if (extraction.path === 'displacement' && extraction.unit === 'm') {
              // Convert kg to tonnes for displacement
              if (typeof value === 'number') {
                processedValue = value / 1000;
                vesselInfo.displacement = processedValue;
              }
            } else {
              // Handle simple numbers and strings
              if (typeof value === 'number' || typeof value === 'string') {
                (vesselInfo as any)[extraction.path] = value;
                processedValue = value;
              } else if (typeof value === 'object' && value !== null) {
                // Handle nested objects - try to extract the actual value
                let extractedValue = null;
                
                // Common patterns in SignalK data
                if (value.overall !== undefined) {
                  extractedValue = value.overall;
                } else if (value.maximum !== undefined) {
                  extractedValue = value.maximum;
                } else if (value.minimum !== undefined) {
                  extractedValue = value.minimum;
                } else if (value.hull !== undefined) {
                  extractedValue = value.hull;
                } else if (value.value !== undefined) {
                  extractedValue = value.value;
                } else {
                  // If it's a simple object with one key, try to extract that value
                  const keys = Object.keys(value);
                  if (keys.length === 1) {
                    extractedValue = value[keys[0]];
                  }
                }
                
                if (extractedValue !== null && extractedValue !== undefined) {
                  (vesselInfo as any)[extraction.path] = extractedValue;
                  processedValue = extractedValue;
                }
              }
            }
            
            this.app.debug(`Extracted ${extraction.displayName}: ${processedValue}`);
          }
        } catch (error) {
          this.app?.debug(`Failed to extract ${extraction.displayName}: ${(error as Error).message}`);
        }
      }

      // Try to extract additional vessel data from other common paths
      await this.extractAdditionalVesselData(vesselInfo);

    } catch (error) {
      this.app?.error(`Failed to extract vessel information: ${(error as Error).message}`);
    }

    return vesselInfo;
  }

  /**
   * Extract additional vessel data from other SignalK paths
   */
  private async extractAdditionalVesselData(vesselInfo: VesselInfo): Promise<void> {
    if (!this.app) return;

    try {
      // Try alternative paths for common data
      if (!vesselInfo.name) {
        const altName = this.app.getSelfPath('registrations.national.registration') || 
                       this.app.getSelfPath('registrations.local.registration');
        if (altName) {
          vesselInfo.name = altName;
        }
      }

      if (!vesselInfo.length) {
        // Try alternative length paths
        let altLength = this.app.getSelfPath('design.length.hull') || 
                       this.app.getSelfPath('design.length.waterline');
                       
        // If we get the whole length object, try to extract a useful value
        if (!altLength) {
          const lengthObj = this.app.getSelfPath('design.length');
          if (lengthObj && typeof lengthObj === 'object') {
            altLength = lengthObj.overall || lengthObj.hull || lengthObj.waterline;
          }
        }
        
        if (altLength && typeof altLength === 'number') {
          vesselInfo.length = altLength;
        }
      }

      if (!vesselInfo.draft) {
        // Try alternative draft paths
        let altDraft = this.app.getSelfPath('design.draft.minimum') || 
                      this.app.getSelfPath('design.draft.current');
                      
        // If we get the whole draft object, try to extract a useful value
        if (!altDraft) {
          const draftObj = this.app.getSelfPath('design.draft');
          if (draftObj && typeof draftObj === 'object') {
            altDraft = draftObj.maximum || draftObj.minimum || draftObj.current;
          }
        }
        
        if (altDraft && typeof altDraft === 'number') {
          vesselInfo.draft = altDraft;
        }
      }

      // Extract additional notes from description fields
      const description = this.app.getSelfPath('design.description');
      if (description && !vesselInfo.notes) {
        vesselInfo.notes = description;
      }

    } catch (error) {
      this.app?.debug(`Failed to extract additional vessel data: ${(error as Error).message}`);
    }
  }

  /**
   * Convert AIS ship type number to readable string
   */
  private convertAISShipType(shipType: number): string {
    const aisTypes: { [key: number]: string } = {
      36: 'Sailing vessel',
      37: 'Pleasure craft',
      31: 'Towing vessel',
      32: 'Towing vessel (length > 200m)',
      33: 'Vessel engaged in dredging',
      34: 'Vessel engaged in diving operations',
      35: 'Military vessel',
      30: 'Fishing vessel',
      20: 'Wing in ground craft',
      21: 'Cargo vessel',
      22: 'Cargo vessel',
      23: 'Cargo vessel',
      24: 'Cargo vessel',
      25: 'Cargo vessel',
      26: 'Cargo vessel',
      27: 'Cargo vessel',
      28: 'Cargo vessel',
      29: 'Cargo vessel',
      70: 'Passenger vessel',
      71: 'Passenger vessel',
      72: 'Passenger vessel',
      73: 'Passenger vessel',
      74: 'Passenger vessel',
      75: 'Passenger vessel',
      76: 'Passenger vessel',
      77: 'Passenger vessel',
      78: 'Passenger vessel',
      79: 'Passenger vessel',
      80: 'Tanker',
      81: 'Tanker',
      82: 'Tanker',
      83: 'Tanker',
      84: 'Tanker',
      85: 'Tanker',
      86: 'Tanker',
      87: 'Tanker',
      88: 'Tanker',
      89: 'Tanker'
    };

    return aisTypes[shipType] || `Unknown vessel type (${shipType})`;
  }

  /**
   * Load vessel context from file
   */
  private async loadVesselContext(): Promise<void> {
    try {
      if (await fs.pathExists(this.contextFilePath)) {
        this.vesselContext = await fs.readJson(this.contextFilePath);
        this.app?.debug(`Loaded vessel context from ${this.contextFilePath}`);
      } else {
        // Create default context
        this.vesselContext = {
          vesselInfo: {},
          customContext: '',
          lastUpdated: new Date().toISOString(),
          autoExtracted: false
        };
        await this.saveVesselContext();
      }
    } catch (error) {
      this.app?.error(`Failed to load vessel context: ${(error as Error).message}`);
      // Create default context on error
      this.vesselContext = {
        vesselInfo: {},
        customContext: '',
        lastUpdated: new Date().toISOString(),
        autoExtracted: false
      };
    }
  }

  /**
   * Save vessel context to file
   */
  async saveVesselContext(): Promise<void> {
    try {
      if (!this.vesselContext) return;

      // Ensure directory exists
      await fs.ensureDir(path.dirname(this.contextFilePath));
      
      // Update last modified time
      this.vesselContext.lastUpdated = new Date().toISOString();
      
      // Save to file
      await fs.writeJson(this.contextFilePath, this.vesselContext, { spaces: 2 });
      
      this.app?.debug(`Saved vessel context to ${this.contextFilePath}`);
    } catch (error) {
      this.app?.error(`Failed to save vessel context: ${(error as Error).message}`);
      throw error;
    }
  }

  /**
   * Get current vessel context - ensure it's loaded first
   */
  async getVesselContext(): Promise<VesselContext | undefined> {
    // Ensure context is loaded
    if (!this.vesselContext) {
      await this.loadVesselContext();
    }
    return this.vesselContext;
  }

  /**
   * Update vessel context with new information
   */
  async updateVesselContext(
    vesselInfo?: Partial<VesselInfo>, 
    customContext?: string,
    autoExtracted: boolean = false
  ): Promise<VesselContext> {
    // Ensure context is loaded first
    if (!this.vesselContext) {
      await this.loadVesselContext();
    }

    // If still no context after loading, create a default one
    if (!this.vesselContext) {
      this.vesselContext = {
        vesselInfo: {},
        customContext: '',
        lastUpdated: new Date().toISOString(),
        autoExtracted: false
      };
    }

    // Update vessel info if provided
    if (vesselInfo) {
      this.vesselContext.vesselInfo = { ...this.vesselContext.vesselInfo, ...vesselInfo };
    }

    // Update custom context if provided
    if (customContext !== undefined) {
      this.vesselContext.customContext = customContext;
    }

    // Set auto-extracted flag
    this.vesselContext.autoExtracted = autoExtracted;

    // Save changes
    await this.saveVesselContext();

    return this.vesselContext;
  }

  /**
   * Refresh vessel information from SignalK
   */
  async refreshVesselInfo(): Promise<VesselContext> {
    const extractedInfo = await this.extractVesselInfo();
    return await this.updateVesselContext(extractedInfo, undefined, true);
  }

  /**
   * Generate context string for Claude AI
   */
  generateClaudeContext(): string {
    if (!this.vesselContext) {
      return '=== VESSEL CONTEXT ===\n\nNo vessel context information available.\n\n=== END VESSEL CONTEXT ===\n';
    }

    const { vesselInfo, customContext } = this.vesselContext;
    const contextParts: string[] = [];

    // Add vessel identification
    contextParts.push('=== VESSEL CONTEXT ===');
    
    if (vesselInfo.name || vesselInfo.callsign || vesselInfo.mmsi) {
      contextParts.push('\n--- VESSEL IDENTIFICATION ---');
      if (vesselInfo.name) contextParts.push(`Vessel Name: ${vesselInfo.name}`);
      if (vesselInfo.callsign) contextParts.push(`Call Sign: ${vesselInfo.callsign}`);
      if (vesselInfo.mmsi) contextParts.push(`MMSI: ${vesselInfo.mmsi}`);
      if (vesselInfo.flag) contextParts.push(`Flag: ${vesselInfo.flag}`);
      if (vesselInfo.port) contextParts.push(`Port of Registry: ${vesselInfo.port}`);
    }

    // Add physical characteristics
    if (vesselInfo.length || vesselInfo.beam || vesselInfo.draft || vesselInfo.height || vesselInfo.displacement) {
      contextParts.push('\n--- PHYSICAL CHARACTERISTICS ---');
      if (vesselInfo.length) contextParts.push(`Length Overall (LOA): ${typeof vesselInfo.length === 'object' ? JSON.stringify(vesselInfo.length) : vesselInfo.length}m`);
      if (vesselInfo.beam) contextParts.push(`Beam: ${typeof vesselInfo.beam === 'object' ? JSON.stringify(vesselInfo.beam) : vesselInfo.beam}m`);
      if (vesselInfo.draft) contextParts.push(`Draft: ${typeof vesselInfo.draft === 'object' ? JSON.stringify(vesselInfo.draft) : vesselInfo.draft}m`);
      if (vesselInfo.height) contextParts.push(`Air Draft/Height: ${vesselInfo.height}m`);
      if (vesselInfo.displacement) contextParts.push(`Displacement: ${vesselInfo.displacement} tonnes`);
    }

    // Add vessel type and classification
    if (vesselInfo.vesselType || vesselInfo.classification) {
      contextParts.push('\n--- VESSEL TYPE & CLASSIFICATION ---');
      if (vesselInfo.vesselType) contextParts.push(`Vessel Type: ${vesselInfo.vesselType}`);
      if (vesselInfo.classification) contextParts.push(`Classification: ${vesselInfo.classification}`);
    }

    // Add technical specifications
    if (vesselInfo.grossTonnage || vesselInfo.netTonnage || vesselInfo.deadWeight) {
      contextParts.push('\n--- TECHNICAL SPECIFICATIONS ---');
      if (vesselInfo.grossTonnage) contextParts.push(`Gross Tonnage: ${vesselInfo.grossTonnage} GT`);
      if (vesselInfo.netTonnage) contextParts.push(`Net Tonnage: ${vesselInfo.netTonnage} NT`);
      if (vesselInfo.deadWeight) contextParts.push(`Deadweight: ${vesselInfo.deadWeight} tonnes`);
    }

    // Add build information
    if (vesselInfo.builder || vesselInfo.buildYear || vesselInfo.hullNumber) {
      contextParts.push('\n--- BUILD INFORMATION ---');
      if (vesselInfo.builder) contextParts.push(`Builder: ${vesselInfo.builder}`);
      if (vesselInfo.buildYear) contextParts.push(`Build Year: ${vesselInfo.buildYear}`);
      if (vesselInfo.hullNumber) contextParts.push(`Hull Number: ${vesselInfo.hullNumber}`);
    }

    // Add contact information
    if (vesselInfo.ownerName) {
      contextParts.push('\n--- CONTACT INFORMATION ---');
      contextParts.push(`Owner: ${vesselInfo.ownerName}`);
    }

    // Add additional notes
    if (vesselInfo.notes) {
      contextParts.push('\n--- ADDITIONAL VESSEL NOTES ---');
      contextParts.push(vesselInfo.notes);
    }

    // Add custom context
    if (customContext && customContext.trim()) {
      contextParts.push('\n--- CUSTOM OPERATIONAL CONTEXT ---');
      contextParts.push(customContext.trim());
    }

    contextParts.push('\n=== END VESSEL CONTEXT ===\n');

    return contextParts.join('\n');
  }

  /**
   * Get available vessel data paths for UI
   */
  static getVesselDataPaths(): VesselContextExtraction[] {
    return [...VesselContextManager.VESSEL_DATA_PATHS];
  }
}