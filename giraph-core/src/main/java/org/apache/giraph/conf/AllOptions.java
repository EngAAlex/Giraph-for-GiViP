/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.conf;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.giraph.conf.GiraphConstants.COMPUTATION_CLASS;

/**
 * Tracks all of the Giraph options
 */
public class AllOptions {
  /**  logger object */
  private static final Logger LOG = Logger.getLogger(AllOptions.class);

  /** Configuration options */
  private static final List<AbstractConfOption> CONF_OPTIONS =
      Lists.newArrayList();

  /** page name for the HTML page generation */
  private static final String PAGE_NAME = "Giraph Options";

  /** Don't construct */
  private AllOptions() { }

  /**
   * Add an option. Subclasses of {@link AbstractConfOption} should call this
   * at the end of their constructor.
   *
   * @param confOption option
   */
  public static void add(AbstractConfOption confOption) {
    CONF_OPTIONS.add(confOption);
  }

  /**
   * String representation of all of the options stored
   *
   * @return string
   */
  public static String allOptionsString() {
    Collections.sort(CONF_OPTIONS);
    StringBuilder sb = new StringBuilder(CONF_OPTIONS.size() * 30);
    sb.append("All Options:\n");
    ConfOptionType lastType = null;
    for (AbstractConfOption confOption : CONF_OPTIONS) {
      if (!confOption.getType().equals(lastType)) {
        sb.append(confOption.getType().toString().toLowerCase()).append(":\n");
        lastType = confOption.getType();
      }
      sb.append(confOption);
    }
    return sb.toString();
  }

  /**
   * HTML String representation of all the options stored
   * @return String the HTML representation of the registered options
   */
  public static String allOptionsHTMLString() {
    Collections.sort(CONF_OPTIONS);
    StringBuilder sb = new StringBuilder(CONF_OPTIONS.size() * 30);

    sb.append("<?xml version='1.0' encoding='UTF-8'?>\n" +
              "<!--\n" +
              "Licensed to the Apache Software Foundation (ASF) under one\n" +
              "or more contributor license agreements.  See the NOTICE file\n" +
              "distributed with this work for additional information\n" +
              "regarding copyright ownership.  The ASF licenses this file\n" +
              "to you under the Apache License, Version 2.0 (the\n" +
              "'License'); you may not use this file except in compliance\n" +
              "with the License.  You may obtain a copy of the License at\n" +
              "\n" +
              "    http://www.apache.org/licenses/LICENSE-2.0\n" +
              "\n" +
              "Unless required by applicable law or agreed to in writing,\n" +
              "software distributed under the License is distributed on an\n" +
              "'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n" +
              "KIND, either express or implied.  See the License for the\n" +
              "specific language governing permissions and limitations\n" +
              "under the License.\n" +
              "-->\n" +
              "\n" +
              "<document xmlns='http://maven.apache.org/XDOC/2.0'\n" +
              "          xmlns:xsi='http://www.w3.org/2001/" +
              "XMLSchema-instance'\n" +
              "          xsi:schemaLocation='" +
              "http://maven.apache.org/XDOC/2.0 " +
              " http://maven.apache.org/xsd/xdoc-2.0.xsd'>\n" +
              "  <properties>\n" +
              "    <title>" + PAGE_NAME + "</title>\n" +
              "  </properties>\n" +
              "  <body>\n" +
              "    <section name='" + PAGE_NAME + "'>\n" +
              "      <table border='0' style='width:110%; max-width:110%'>\n" +
              "       <tr>\n" +
              "        <th>label</th>\n" +
              "        <th>type</th>\n" +
              "        <th>default value</th>\n" +
              "        <th>description</th>\n" +
              "       </tr>\n");

    for (AbstractConfOption confOption : CONF_OPTIONS) {
      String type = confOption.getType().toString().toLowerCase();

      sb.append("       <tr>\n");
      sb.append("         <td>" + confOption.getKey() + "</td>\n");
      sb.append("         <td>" + type  + "</td>\n");
      sb.append("         <td>" + confOption.getDefaultValueStr() + "</td>\n");
      sb.append("         <td>" + confOption.getDescription() + "</td>\n");
      sb.append("       </tr>\n");
    }

    sb.append("      </table>\n" +
              "    </section>\n" +
              "  </body>\n" +
              "</document>\n");

    return sb.toString();
  }

  /**
   * Command line utility to dump all Giraph options
   *
   * @param args cmdline args
   */
  public static void main(String[] args) {
    // This is necessary to trigger the static constants in GiraphConstants to
    // get loaded. Without it we get no output.
    COMPUTATION_CLASS.toString();

    // in case an options was specified, this option is treated as the output
    // file in which to write the HTML version of the list of available options
    if (args.length == 1) {
      String html = allOptionsHTMLString();

      try {
        FileWriter     fs  = new FileWriter(args[0]);
        BufferedWriter out = new BufferedWriter(fs);

        out.write(html);
        out.close();

      } catch (IOException e) {
        LOG.error("Error: " + e.getMessage());
      }
    }

    LOG.info(allOptionsString());
  }
}
