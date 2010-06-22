package org.jumpmind.symmetric.ddl.io;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.commons.lang.exception.NestableRuntimeException;

/**
 * Exception generated by the {@link org.jumpmind.symmetric.ddl.io.DataWriter}.
 * 
 * @version $Revision: 289996 $
 */
public class DataWriterException extends NestableRuntimeException
{
    /** Unique id for serialization purposes. */
    private static final long serialVersionUID = 6254759931565130848L;

    /**
     * Creates a new exception object.
     */
    public DataWriterException()
    {
        super();
    }

    /**
     * Creates a new exception object.
     * 
     * @param message The exception message
     */
    public DataWriterException(String message)
    {
        super(message);
    }

    /**
     * Creates a new exception object.
     * 
     * @param baseEx The base exception
     */
    public DataWriterException(Throwable baseEx)
    {
        super(baseEx);
    }

    /**
     * Creates a new exception object.
     * 
     * @param message The exception message
     * @param baseEx  The base exception
     */
    public DataWriterException(String message, Throwable baseEx)
    {
        super(message, baseEx);
    }
}
