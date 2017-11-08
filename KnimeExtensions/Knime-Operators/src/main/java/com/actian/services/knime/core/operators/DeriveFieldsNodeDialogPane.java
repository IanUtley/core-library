/*
   Copyright 2015 Actian Corporation
 
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.actian.services.knime.core.operators;

import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentMultiLineString;

import com.pervasive.datarush.knime.core.framework.AbstractDRNodeDialogPane;
import com.pervasive.datarush.operators.record.DeriveFields;

final class DeriveFieldsNodeDialogPane extends AbstractDRNodeDialogPane<DeriveFields> {
	  private final DeriveFieldsNodeSettings settings = new DeriveFieldsNodeSettings();
	  
	  public DeriveFieldsNodeDialogPane()
	  {
	    super(new DeriveFields());
	    
	    DialogComponentMultiLineString expressions = new DialogComponentMultiLineString(this.settings.expressions, "Expressions", true, 60, 10);
	    DialogComponentBoolean dropUnderived = new DialogComponentBoolean(settings.dropUnderived, "Drop underived fields?");
	    addDialogComponent(expressions);
	    addDialogComponent(dropUnderived);
	    
	    setDefaultTabTitle("DeriveFields Properties");
	  }
	  
	  protected DeriveFieldsNodeSettings getSettings()
	  {
	    return this.settings;
	  }
	  
	  protected boolean isMetadataRequiredForConfiguration(int portIndex)
	  {
	    return true;
	  }
	}
