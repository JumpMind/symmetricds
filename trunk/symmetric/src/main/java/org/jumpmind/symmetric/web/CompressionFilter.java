/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Keith Naas <knaas@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.iterators.EnumerationIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * Configured within symmetric-web.xml
 * 
 * <pre>
 *  &lt;bean id=&quot;compressionFilter&quot;
 *  class=&quot;org.jumpmind.symmetric.web.CompressionFilter&quot;&gt;
 *    &lt;property name=&quot;regexPattern&quot; value=&quot;string&quot; /&gt;
 *    &lt;property name=&quot;regexPatterns&quot;&gt;
 *      &lt;list&gt;
 *        &lt;value value=&quot;string&quot;/&gt;
 *      &lt;list/&gt;
 *    &lt;property/&gt;
 *    &lt;property name=&quot;uriPattern&quot; value=&quot;string&quot; /&gt;
 *    &lt;property name=&quot;uriPatterns&quot;&gt;
 *      &lt;list&gt;
 *        &lt;value value=&quot;string&quot;/&gt;
 *      &lt;list/&gt;
 *    &lt;property/&gt;
 *    &lt;property name=&quot;disabled&quot; value=&quot;boolean&quot; /&gt;
 *    &lt;property name=&quot;compressionThreshold&quot; value=&quot;int&quot; /&gt;
 *  &lt;/bean&gt;
 * </pre>
 */
public class CompressionFilter extends AbstractFilter {
    
    private static final Log logger = LogFactory.getLog(CompressionFilter.class);

    private org.jumpmind.symmetric.web.compression.CompressionFilter delegate;

    protected int compressionThreshold;

    @Override
    public boolean isContainerCompatible()
    {
        return true;
    }
    
    public void doFilter(ServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException {
        if (delegate != null) {
            delegate.doFilter(request, response, chain);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        if (delegate != null) {
            delegate.destroy();
        }
    }

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
        super.init(filterConfig);
        delegate = new org.jumpmind.symmetric.web.compression.CompressionFilter();

        delegate.init(new CompressionFilterConfig(filterConfig));
    }

    @Override
    protected Log getLogger()
    {
        return logger;
    }
    
    private final class CompressionFilterConfig implements FilterConfig {
        private final FilterConfig filterConfig;
        private final List<String> initParameterNames;

        @SuppressWarnings("unchecked")
        private CompressionFilterConfig(FilterConfig filterConfig) {
            this.filterConfig = filterConfig;
            initParameterNames = IteratorUtils.toList(new EnumerationIterator(
                    filterConfig.getInitParameterNames()));
            if (compressionThreshold > 0) {
                initParameterNames.add("compressionThreshold");
            }
        }

        public String getFilterName() {
            return filterConfig.getFilterName();
        }

        public String getInitParameter(String name) {
            if (compressionThreshold > 0 && "compressionThreshold".equals(name)) {
                return Integer.toString(compressionThreshold);
            }
            return filterConfig.getInitParameter(name);
        }

        public Enumeration<?> getInitParameterNames() {
            return Collections.enumeration(initParameterNames);
        }

        public ServletContext getServletContext() {
            return filterConfig.getServletContext();
        }
    }

    public void setCompressionThreshold(int compressionThreshold) {
        this.compressionThreshold = compressionThreshold;
    }
}
