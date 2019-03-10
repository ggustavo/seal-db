package graphicalInterface.util;


import java.awt.*;

/**
 * A layout manager that implements an extensible grid (mesh): cells in the same
 * row get the same height and cells in the same column get the same width.
 * The row heights/column widths are calculated calling getPreferredSize on the
 * components of the row/column and set to the maximum of the heights/widths.
 * See the document "<a href="doc-files/MeshLayoutHowTo.html">How to use MeshLayout</a>"
 * for more information.
 *
 * <P><DL>
 * <DT><B>License:</B></DT>
 * <DD><pre>
 *  Copyright © 2006, 2007 Roberto Mariottini. All rights reserved.
 *
 *  Permission is granted to anyone to use this software in source and binary forms
 *  for any purpose, with or without modification, including commercial applications,
 *  and to alter it and redistribute it freely, provided that the following conditions
 *  are met:
 *
 *  o  Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *  o  The origin of this software must not be misrepresented; you must not
 *     claim that you wrote the original software. If you use this software
 *     in a product, an acknowledgment in the product documentation would be
 *     appreciated but is not required.
 *  o  Altered source versions must be plainly marked as such, and must not
 *     be misrepresented as being the original software.
 * 
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
 *  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 *  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 *  OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 *  HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 *  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 *  OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 *  SUCH DAMAGE.
 * <pre></DD></DL>
 *
 * @author Roberto Mariottini
 */
public final class MeshLayout implements LayoutManager2
{
  private static final boolean DEBUG = false;

  private static final int DIRECTION_HORIZONTAL = 1;
  private static final int DIRECTION_VERTICAL = 2;
  
  private static final int DEFAULT_GAP = 4;

  private final int direction;
  private final int hgap;
  private final int vgap;
  private final Insets insets;

  private int rows;
  private int cols;

  private int expandRow = -1;
  private int expandCol = -1;

  /**
   * Builds a MeshLayout with the specified rows and columns,
   * with the default gap and with the default margins.
   * The layout placement order will be vertical if cols is zero,
   * horizontal otherwise. One of rows and cols must be > 0.
   *
   * @param rows the number of rows, 0 means unspecified
   * @param cols the number of columns, 0 means unspecified
   */
  public MeshLayout(int rows, int cols)
  {
    this(rows, cols, DEFAULT_GAP, DEFAULT_GAP, null);
  }

  /**
   * Builds a MeshLayout with the specified rows and columns
   * and with the specified gap, used also for the margins.
   * The layout placement order will be vertical if cols is zero,
   * horizontal otherwise. One of rows and cols must be > 0.
   *
   * @param rows the number of rows, 0 means unspecified
   * @param cols the number of columns, 0 means unspecified
   * @param gap the gap, used for both vertical and horizontal
   *            spacing and also for margins
   */
  public MeshLayout(int rows, int cols, int gap)
  {
    this(rows, cols, gap, gap, null);
  }

  /**
   * Builds a MeshLayout with the specified rows and columns
   * and with the specified vertical and horizontal gap, used also
   * for the margins.
   * The layout placement order will be vertical if cols is zero,
   * horizontal otherwise. One of rows and cols must be > 0.
   *
   * @param rows the number of rows, 0 means unspecified
   * @param cols the number of columns, 0 means unspecified
   * @param hgap the horizontal gap, used for horizontal
   *             spacing and also for horizontal margins
   * @param vgap the vertical gap, used for vertical
   *             spacing and also for vertical margins
   */
  public MeshLayout(int rows, int cols, int hgap, int vgap)
  {
    this(rows, cols, hgap, vgap, null);
  }

  /**
   * Builds a MeshLayout with the specified rows and columns,
   * with the default gap, and with the specified margins.
   * The layout placement order will be vertical if cols is zero,
   * horizontal otherwise. One of rows and cols must be > 0.
   *
   * @param rows the number of rows, 0 means unspecified
   * @param cols the number of columns, 0 means unspecified
   * @param margins the margins to use
   */
  public MeshLayout(int rows, int cols, Insets margins)
  {
    this(rows, cols, DEFAULT_GAP, DEFAULT_GAP, margins);
  }

  /**
   * Builds a MeshLayout with the specified rows and columns
   * and with the specified gap and margins.
   * The layout placement order will be vertical if cols is zero,
   * horizontal otherwise. One of rows and cols must be > 0.
   *
   * @param rows the number of rows, 0 means unspecified
   * @param cols the number of columns, 0 means unspecified
   * @param gap the gap, used for both vertical and horizontal spacing
   * @param margins the margins to use
   */
  public MeshLayout(int rows, int cols, int gap, Insets margins)
  {
    this(rows, cols, gap, gap, margins);
  }
  
  /**
   * Builds a MeshLayout with the specified rows and columns
   * and with the specified vertical and horizontal gap and margins.
   * The layout placement order will be vertical if cols is zero,
   * horizontal otherwise. One of rows and cols must be > 0.
   *
   * @param rows the number of rows, 0 means unspecified
   * @param cols the number of columns, 0 means unspecified
   * @param hgap the horizontal gap, used for horizontal spacing
   * @param vgap the vertical gap, used for vertical spacing
   * @param margins the margins to use
   */
  public MeshLayout(int rows, int cols, int hgap, int vgap, Insets margins)
  {
    this.rows = rows;
    this.cols = cols;
    this.hgap = hgap;
    this.vgap = vgap;
    this.insets = margins;
    if (cols == 0)
    {
      direction = DIRECTION_VERTICAL;
    }
    else
    {
      direction = DIRECTION_HORIZONTAL;
    }
    
    if (cols < 0 || rows < 0 || cols + rows == 0)
    {
      throw new IllegalArgumentException("the number of rows and columns must be positive and not both zero");
    }
  }
  
  /**
   * Sets the row that will expand vertically when the container is resized.
   * 
   * @param expandRow the row index (zero-based)
   */
  public void setExpandRow(int expandRow)
  {
    this.expandRow = expandRow;
  }

  /**
   * Sets the column that will expand horizontally when the container is resized.
   * 
   * @param expandCol the column index (zero-based)
   */
  public void setExpandColumn(int expandCol)
  {
    this.expandCol = expandCol;
  }

  // Called by the Container add methods. Layout managers that don't associate strings with their components
  // generally do nothing in this method.
  public void addLayoutComponent(String x, Component comp)
  {
    if (DEBUG) System.out.println("addLayoutComponent() x: " + x + " comp: " + comp.getClass());
  }

  // Called by the Container remove and removeAll methods. Many layout managers do nothing in this method,
  // relying instead on querying the container for its components, using the Container method getComponents
  // (in the API reference documentation).
  public void removeLayoutComponent(Component comp)
  {
    if (DEBUG) System.out.println("removeLayoutComponent() comp: " + comp.getClass());
  }

  // Called by the Container getPreferredSize method, which is itself called under a variety of circumstances.
  // This method should calculate and return the ideal size of the container, assuming that the components it
  // contains will be at or above their preferred sizes. This method must take into account the container's
  // internal borders, which are returned by the getInsets (in the API reference documentation) method.
  public Dimension preferredLayoutSize(Container cont)
  {
    return doWork(cont, false);
  }

  // Called by the Container getMinimumSize method, which is itself called under a variety of circumstances.
  // This method should calculate and return the minimum size of the container, assuming that the components
  // it contains will be at or above their minimum sizes. This method must take into account the container's
  // internal borders, which are returned by the getInsets method.
  public Dimension minimumLayoutSize(Container cont)
  {
    return doWork(cont, false);
  }

  // Called when the container is first displayed, and each time its size changes. A layout manager's
  // layoutContainer method doesn't actually draw components. It simply invokes each component's setSize,
  // setLocation, and setBounds methods to set the component's size and position.
  // This method must take into account the container's internal borders, which are returned by the getInsets
  // method. If appropriate, it should also take the container's orientation (returned by the
  // getComponentOrientation (in the API reference documentation) method) into account. You can't assume that
  // the preferredLayoutSize or minimumLayoutSize method will be called before layoutContainer is called. 
  public void layoutContainer(Container cont)
  {
    doWork(cont, true);
  }
  
  // Adds the specified component to the layout, using the specified constraint object.
  public void addLayoutComponent(Component comp, Object constr)
  {
    if (DEBUG) System.out.println("addLayoutComponent() constr: " + constr + " comp: " + comp.getClass());

    if (constr != null)
    {
      throw new IllegalArgumentException("no constraints allowed here.");
    }
  }
  
  // Returns the alignment along the x axis.
  public float getLayoutAlignmentX(Container target)
  {
    return 0.5f;
  }
  
  // Returns the alignment along the y axis.
  public float getLayoutAlignmentY(Container target)
  {
    return 0.5f;
  }
  
  // Invalidates the layout, indicating that if the layout manager has cached information it should be discarded.
  public void invalidateLayout(Container target)
  {
    if (DEBUG) System.out.println("invalidateLayout() target: " + target);
  }
  
  // Calculates the maximum size dimensions for the specified container, given the components it contains.
  public Dimension maximumLayoutSize(Container target)
  {
    return new Dimension(Integer.MAX_VALUE, Integer.MAX_VALUE);
  }
  
  // do the work (generic)
  private final Dimension doWork(Container cont, boolean setBounds)
  {
    if (DEBUG) System.out.println("===============================");
    if (DEBUG) System.out.println("insets: " + insets + " hgap: " + hgap + " vgap: " + vgap);

    int top = 0;
    int left = 0;
    int bottom = 0;
    int right = 0;

    // calculate insets
    if (insets != null)
    {
      top += insets.left;
      left += insets.top;
      bottom += insets.bottom;
      right += insets.right;
    }
    else
    {
      top += vgap;
      left += hgap;
      bottom += vgap;
      right += hgap;
    }
    
    // calculate container insets (border)
    Insets contInsets = cont.getInsets();
    if (DEBUG) System.out.println("cont.getInsets(): " + contInsets);
    top += contInsets.top;
    left += contInsets.left;
    bottom += contInsets.bottom;
    right += contInsets.right;
    if (DEBUG) System.out.println("top: " + top + " left: " + left + " bottom: " + bottom + " right: " + right);

    // do the work
    Component[] comps = cont.getComponents();
    int[] colWidth = null;
    int[] rowHeight = null;
    if (direction == DIRECTION_HORIZONTAL)
    {
      colWidth = new int[cols];
      int len = comps.length / cols;
      if (comps.length % cols != 0)
      {
        ++len;
      }
      rowHeight = new int[len];
    }
    else
    {
      rowHeight = new int[rows];
      int len = comps.length / rows;
      if (comps.length % rows != 0)
      {
        ++len;
      }
      colWidth = new int[len];
    }
    int r = 0;
    int c = 0;
    for (int i = 0; i < comps.length; ++i)
    {
      Dimension d = comps[i].getPreferredSize();
      if (DEBUG) System.out.println("comps[" + i + "] : " + comps[i].getClass() + " .getPreferredSize(): " + d);
      if (d.height > rowHeight[r])
      {
        rowHeight[r] = d.height;
      }
      if (d.width > colWidth[c])
      {
        colWidth[c] = d.width;
      }
      if (direction == DIRECTION_HORIZONTAL)
      {
        ++c;
        if (c >= cols)
        {
          c = 0;
          ++r;
        }
      }
      else
      {
        ++r;
        if (r >= rows)
        {
          r = 0;
          ++c;
        }
      }
    }
    if (setBounds)
    {
      // calclulate expandRow and expandCol
      Dimension contSize = cont.getSize();
      if (expandRow >= 0  &&  expandRow < rowHeight.length)
      {
        int height = top;
        for (r = 0; r < rowHeight.length - 1; ++r)
        {
          height += rowHeight[r] + vgap;
        }
        height += rowHeight[r] + bottom;
        int delta = contSize.height - height;
        if (delta > 0)
        {
          rowHeight[expandRow] += delta;
        }
      }
      if (expandCol >= 0  &&  expandCol < colWidth.length)
      {
        int width = left;
        for (c = 0; c < colWidth.length - 1; ++c)
        {
          width += colWidth[c] + hgap;
        }
        width += colWidth[c] + right;
        int delta = contSize.width - width;
        if (delta > 0)
        {
          colWidth[expandCol] += delta;
        }
      }

      // do the layout
      r = 0;
      c = 0;
      int vpos = top;
      int hpos = left;
      for (int i = 0; i < comps.length; ++i)
      {
        comps[i].setBounds(hpos, vpos, colWidth[c], rowHeight[r]);
        if (DEBUG) System.out.println("comps[" + i + "] : " + comps[i].getClass() + " .setBounds(): " + hpos + ", " + vpos + ", " + colWidth[c] + ", " + rowHeight[r]);
        if (direction == DIRECTION_HORIZONTAL)
        {
          hpos += colWidth[c] + hgap;
          ++c;
          if (c >= cols)
          {
            c = 0;
            hpos = left;
            vpos += rowHeight[r] + vgap;
            ++r;
          }
        }
        else
        {
          vpos += rowHeight[r] + vgap;
          ++r;
          if (r >= rows)
          {
            r = 0;
            vpos = top;
            hpos += colWidth[c] + hgap;
            ++c;
          }
        }
      }
    }
    else
    {
      int width = left;
      for (c = 0; c < colWidth.length - 1; ++c)
      {
        width += colWidth[c] + hgap;
      }
      width += colWidth[c] + right;
      int height = top;
      for (r = 0; r < rowHeight.length - 1; ++r)
      {
        height += rowHeight[r] + vgap;
      }
      height += rowHeight[r] + bottom;
      if (DEBUG) System.out.println("Minimum dimensions: " + width + ", " + height);
      return new Dimension(width, height);
    }
    return null;
  }
}
