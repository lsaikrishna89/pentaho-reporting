/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.reporting.designer.core.auth;

import org.pentaho.reporting.designer.core.settings.prefs.PreferencesMap;
import org.pentaho.reporting.libraries.base.util.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Properties;

/**
 * Todo: Document me!
 *
 * @author Thomas Morgner.
 */
public class ReportAuthenticationStore implements AuthenticationStore {
  private AuthenticationStore backend;
  private HashMap<String, AuthenticationData> localStorage;

  public ReportAuthenticationStore( final AuthenticationStore backend ) {
    if ( backend == null ) {
      throw new NullPointerException();
    }
    this.backend = backend;
    this.localStorage = new HashMap<String, AuthenticationData>();
  }

  public String getUsername( final String url ) {
    final AuthenticationData data = localStorage.get( url );
    if ( data != null ) {
      return data.getOption( USER_KEY );
    }
    return backend.getUsername( url );
  }

  public String getPassword( final String url ) {
    final AuthenticationData data = localStorage.get( url );
    if ( data != null ) {
      return data.getOption( PASSWORD_KEY );
    }
    return backend.getPassword( url );
  }

  public String getOption( final String url, final String key ) {
    final AuthenticationData data = localStorage.get( url );
    if ( data != null ) {
      return data.getOption( key );
    }
    return backend.getOption( url, key );
  }

  public String[] getDefinedOptions( final String url ) {
    final AuthenticationData data = localStorage.get( url );
    if ( data == null ) {
      return backend.getDefinedOptions( url );
    }
    final LinkedHashSet<String> keys = new LinkedHashSet<String>();
    keys.addAll( Arrays.asList( backend.getDefinedOptions( url ) ) );
    keys.addAll( Arrays.asList( data.getDefinedOptions() ) );
    return keys.toArray( new String[ keys.size() ] );
  }

  public String[] getKnownURLs() {
    final LinkedHashSet<String> keys = new LinkedHashSet<String>();
    keys.addAll( Arrays.asList( backend.getKnownURLs() ) );
    keys.addAll( Arrays.asList( localStorage.keySet().toArray( new String[ localStorage.size() ] ) ) );
    return keys.toArray( new String[ keys.size() ] );
  }


  public void addCredentials( final String url,
                              final String user,
                              final String password,
                              final Properties options,
                              final boolean persist ) {
    add( GlobalAuthenticationStore.createAuthenticationData( url, user, password, options ), persist );
  }

  public void add( final AuthenticationData data, final boolean persist ) {
    if ( persist ) {
      backend.add( data, true );
    }


    final PreferencesMap.ConfigurationData oldData = localStorage.get( data.getKey() );
    if ( oldData != null ) {
      final String[] strings = oldData.getDefinedOptions();
      for ( int i = 0; i < strings.length; i++ ) {
        final String key = strings[ i ];
        if ( data.getOption( key ) == null ) {
          data.setOption( key, oldData.getOption( key ) );
        }
      }
    }

    localStorage.put( data.getKey(), data );
  }

  public void removeCredentials( final String url ) {
    backend.removeCredentials( url );
  }

  public AuthenticationData getCredentials( final String url ) {
    final AuthenticationData authenticationData = localStorage.get( url );
    if ( authenticationData != null ) {
      return authenticationData;
    }
    return backend.getCredentials( url );
  }

  public int getIntOption( final String path, final String key, final int defaultValue ) {
    final String option = getOption( path, key );
    if ( StringUtils.isEmpty( option ) ) {
      return defaultValue;
    }
    try {
      return Integer.parseInt( option );
    } catch ( Exception e ) {
      return defaultValue;
    }
  }
}
