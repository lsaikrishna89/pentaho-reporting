/*!
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
 */

package org.pentaho.reporting.engine.classic.core.modules.parser.base;

import org.pentaho.reporting.libraries.base.util.PasswordObscurification;

public class ObscurificatePasswordEncryptionServiceProvider implements PasswordEncryptionServiceProvider {
  public static final String SERVICE_TAG = "encrypted";

  public ObscurificatePasswordEncryptionServiceProvider() {
  }

  public String getPrefix() {
    return SERVICE_TAG;
  }

  public String encrypt( final String rawPassword ) {
    return PasswordObscurification.encryptPassword( rawPassword );
  }

  public String decrypt( final String encryptedPassword ) {
    if ( encryptedPassword == null ) {
      return null;
    }
    if ( "".equals( encryptedPassword ) ) {
      return "";
    }
    return PasswordObscurification.decryptPassword( encryptedPassword );
  }
}