/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */
package org.jumpmind.symmetric.util;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;

import bsh.EvalError;
import bsh.Interpreter;

/**
 * General application utility methods
 */
public class AppUtils {

    private static String UNKNOWN = "unknown";

    private static ILog log = LogFactory.getLog(AppUtils.class);

    private static final String SYM_TEMP_SUFFIX = "sym.tmp";

    private static String serverId;

    private static FastDateFormat timezoneFormatter = FastDateFormat.getInstance("Z");

    private static Pattern pattern = Pattern.compile("\\$\\((.+?)\\)");

    /**
     * Get a unique identifier that represents the JVM instance this server is
     * currently running in.
     */
    public static String getServerId() {
        if (StringUtils.isBlank(serverId)) {
            serverId = System.getProperty("runtime.symmetric.cluster.server.id", null);
            if (StringUtils.isBlank(serverId)) {
                // JBoss uses this system property to identify a server in a
                // cluster
                serverId = System.getProperty("bind.address", null);
                if (StringUtils.isBlank(serverId)) {
                    try {
                        serverId = getHostName();
                    } catch (Exception ex) {
                        serverId = "unknown";
                    }
                }
            }
        }
        return serverId;
    }

    public static String getHostName() {
        String hostName = System.getProperty("host.name", UNKNOWN);
        if (UNKNOWN.equals(hostName)) {
            try {
                hostName = InetAddress.getLocalHost().getHostName();
            } catch (Exception ex) {
                log.warn(ex);
            }
        }
        return hostName;
    }

    public static String getIpAddress() {
        String ipAddress = System.getProperty("ip.address", UNKNOWN);
        if (UNKNOWN.equals(ipAddress)) {
            try {
                ipAddress = InetAddress.getLocalHost().getHostAddress();
            } catch (Exception ex) {
                log.warn(ex);
            }
        }
        return ipAddress;
    }

    public static String replace(String prop, String replaceWith, String sourceString) {
        return StringUtils.replace(sourceString, "$(" + prop + ")", replaceWith);
    }

    public static String replaceTokens(String text, Map<String, String> replacements,
            boolean matchUsingPrefixSuffix) {
        if (replacements != null && replacements.size() > 0) {
            if (matchUsingPrefixSuffix) {
                Matcher matcher = pattern.matcher(text);
                StringBuffer buffer = new StringBuffer();
                while (matcher.find()) {
                    String[] match = matcher.group(1).split("\\|");
                    String replacement = replacements.get(match[0]);
                    if (replacement != null) {
                        matcher.appendReplacement(buffer, "");
                        if (match.length == 2) {
                            replacement = formatString(match[1], replacement);
                        }
                        buffer.append(replacement);
                    }
                }
                matcher.appendTail(buffer);
                text = buffer.toString();
            } else {
                for (Object key : replacements.keySet()) {
                    text = text.replaceAll(key.toString(), replacements.get(key));
                }
            }
        }
        return text;

    }

    public static String formatString(String format, String arg) {
        if (format.indexOf("d") >= 0 || format.indexOf("u") >= 0 || format.indexOf("i") >= 0) {
            return String.format(format, Long.parseLong(arg));
        } else if (format.indexOf("e") >= 0 || format.indexOf("f") >= 0) {
            return String.format(format, Double.valueOf(arg));
        } else {
            return String.format(format, arg);
        }
    }

    /**
     * This method will return the timezone in RFC822 format. </p> The format
     * ("-+HH:MM") has advantages over the older timezone codes ("AAA"). The
     * difference of 5 hours from GMT is obvious with "-05:00" but only implied
     * with "EST". There is no ambiguity saying "-06:00", but you don't know if
     * "CST" means Central Standard Time ("-06:00") or China Standard Time
     * ("+08:00"). The timezone codes need to be loaded on the system, and
     * definitions are not standardized between systems. Therefore, to remain
     * agnostic to operating systems and databases, the RFC822 format is the
     * best choice.
     */
    public static String getTimezoneOffset() {
        String tz = timezoneFormatter.format(new Date());
        if (tz != null && tz.length() == 5) {
            return tz.substring(0, 3) + ":" + tz.substring(3, 5);
        }
        return null;
    }

    /**
     * Handy utility method to look up a SymmetricDS component given the bean
     * name.
     * 
     * @see Constants
     */
    @SuppressWarnings("unchecked")
    public static <T> T find(String name, ISymmetricEngine engine) {
        return (T) engine.getApplicationContext().getBean(name);
    }

    /**
     * Use this method to create any needed temporary files for SymmetricDS.
     */
    public static File createTempFile(String token) throws IOException {
        return File.createTempFile(token + ".", "." + SYM_TEMP_SUFFIX);
    }

    /**
     * Clean up files created by {@link #createTempFile(String)}. This only be
     * called while the engine is not synchronizing!
     */
    @SuppressWarnings("unchecked")
    public static void cleanupTempFiles() {
        try {
            File tmp = File.createTempFile("temp.", "." + SYM_TEMP_SUFFIX);
            Iterator<File> it = FileUtils.iterateFiles(tmp.getParentFile(),
                    new String[] { SYM_TEMP_SUFFIX }, true);
            int deletedCount = 0;
            while (it.hasNext()) {
                try {
                    FileUtils.forceDelete(it.next());
                    deletedCount++;
                } catch (Exception ex) {
                    log.error(ex);
                }
            }
            if (deletedCount > 1) {
                log.warn("CleanStrandedTempFiles", deletedCount);
            }
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    /**
     * @param timezoneOffset
     *            see description for {@link #getTimezoneOffset()}
     * @return a date object that represents the local date and time at the
     *         passed in offset
     */
    public static Date getLocalDateForOffset(String timezoneOffset) {
        long currentTime = System.currentTimeMillis();
        int myOffset = TimeZone.getDefault().getOffset(currentTime);
        int theirOffset = TimeZone.getTimeZone("GMT" + timezoneOffset).getOffset(currentTime);
        return new Date(currentTime - myOffset + theirOffset);
    }

    /**
     * Useful method to sleep that catches and ignores the
     * {@link InterruptedException}
     * 
     * @param ms
     *            milliseconds to sleep
     */
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            log.warn("Message", e.getMessage());
        }
    }

    /**
     * Attempt to close all the connections to a database that a DataSource has.
     * This method should not be relied upon as it only works with certain
     * {@link DataSource} implementations.
     */
    public static void resetDataSource(DataSource ds) {
        if (ds instanceof BasicDataSource) {
            BasicDataSource bds = (BasicDataSource) ds;
            try {
                bds.close();
            } catch (Exception ex) {
                log.warn(ex);
            }
        }
    }

    public static boolean isSystemPropertySet(String propName, boolean defaultValue) {
        return "true"
                .equalsIgnoreCase(System.getProperty(propName, Boolean.toString(defaultValue)));
    }

    public static void runBsh(String script) {
        try {
            Interpreter interpreter = new Interpreter();
            interpreter.eval(script);
        } catch (EvalError e) {
            throw new RuntimeException(e);
        }
    }

}