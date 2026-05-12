import pandas as pd
import numpy as np
import logging

class ProjectionEngine:
    """
    Handles all sales projection logic for WOS calculations.
    This centralizes the business logic for how we forecast future demand.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def calculate_style_projection(self, df, ly_df, current_week_df, 
                                    style_column='internal_style_color',
                                    latest_week=None, ly_current=None, ly_next=None):
        """
        Calculate style-level projections, then distribute to sizes using
        adaptive size mix that considers recent momentum, LY patterns, and inventory.
        
        This handles the complexity of style-based purchasing with size-level inventory.
        
        Parameters:
        -----------
        df : DataFrame
            13 weeks of TY data with store/item/style
        ly_df : DataFrame
            13 weeks of LY data with store/item/style
        current_week_df : DataFrame
            Current week inventory data
        style_column : str
            Column name that identifies the style (default: 'internal_style_color')
        
        Returns:
        --------
        DataFrame with size-level projections based on style-level demand
        
        Business Logic:
        ---------------
        1. Aggregate demand to store/style level (customers buy styles, not sizes)
        2. Calculate style-level growth rate (TY 13wk vs LY 13wk)
        3. Project style-level demand using LY actuals x growth rate
        4. Calculate size mix from multiple signals:
        a. Recent momentum: TY 13-week size mix
        b. Historical pattern: LY 13-week size mix
        c. Inventory reality: Current pipeline size distribution
        5. Blend size mixes adaptively based on style performance
        6. Distribute style projection to sizes using blended mix
        """
        
        self.logger.info("="*60)
        self.logger.info("STYLE-LEVEL PROJECTION ENGINE")
        self.logger.info("="*60)
        
        # =========================================================================
        # STEP 1: AGGREGATE TO STYLE LEVEL FOR DEMAND PROJECTION
        # =========================================================================
        self.logger.info("\nStep 1: Aggregating to style level...")

        # TY: First sum to week/style level (collapse sizes), then average across weeks
        weekly_style_ty = df.groupby(['store_number', style_column, 'f5c_calendar_week']).agg({
            'pos_quantity_this_year': 'sum'
        }).reset_index()

        style_ty = weekly_style_ty.groupby(['store_number', style_column]).agg({
            'pos_quantity_this_year': 'mean'
        }).reset_index()
        style_ty.columns = ['store_number', style_column, 'style_ty_13wk_avg']

        self.logger.info(f"\nDEBUG Step 1 - TY aggregation:")
        self.logger.info(f"  Input df rows: {len(df)}")
        self.logger.info(f"  Weekly style rows: {len(weekly_style_ty)}")
        self.logger.info(f"  Final style_ty rows: {len(style_ty)}")
        self.logger.info(f"  Sample style_ty_13wk_avg: {style_ty['style_ty_13wk_avg'].iloc[0]:.4f}")
        self.logger.info(f"  Total TY avg per week: {style_ty['style_ty_13wk_avg'].sum():.2f}")

        # =========================================================================
        # STEP 2: CALCULATE STYLE-LEVEL GROWTH RATE
        # =========================================================================
        self.logger.info("\nStep 2: Calculating style-level growth rates...")

        # LY: First sum to week/style level (collapse sizes), then average across weeks
        if len(ly_df) > 0:
            weekly_style_ly = ly_df.groupby(['store_number', style_column, 'f5c_calendar_week']).agg({
                'pos_quantity_this_year': 'sum'
            }).reset_index()
            
            style_ly = weekly_style_ly.groupby(['store_number', style_column]).agg({
                'pos_quantity_this_year': 'mean'
            }).reset_index()
            style_ly.columns = ['store_number', style_column, 'style_ly_13wk_avg']
            
            self.logger.info(f"\nDEBUG Step 2 - LY aggregation:")
            self.logger.info(f"  Input ly_df rows: {len(ly_df)}")
            self.logger.info(f"  Weekly style LY rows: {len(weekly_style_ly)}")
            self.logger.info(f"  Final style_ly rows: {len(style_ly)}")
            self.logger.info(f"  Sample style_ly_13wk_avg: {style_ly['style_ly_13wk_avg'].iloc[0]:.4f}")
            self.logger.info(f"  Total LY avg per week: {style_ly['style_ly_13wk_avg'].sum():.2f}")
        else:
            self.logger.warning("No LY data available - creating empty dataframe")
            # No LY data - create empty dataframe with same structure as style_ty
            style_ly = style_ty.copy()
            style_ly['style_ly_13wk_avg'] = 0
            style_ly = style_ly[['store_number', style_column, 'style_ly_13wk_avg']]

        # Merge TY and LY
        style_data = style_ty.merge(style_ly, on=['store_number', style_column], how='left')
        style_data['style_ly_13wk_avg'] = style_data['style_ly_13wk_avg'].fillna(0)

        self.logger.info(f"  Unique store/style combinations: {len(style_data):,}")

        # Calculate style-level growth rate
        style_data['style_growth_rate'] = np.where(
            style_data['style_ly_13wk_avg'] > 0,
            (style_data['style_ty_13wk_avg'] - style_data['style_ly_13wk_avg']) / style_data['style_ly_13wk_avg'],
            0
        )

        # Classify style performance
        style_data['style_performance'] = pd.cut(
            style_data['style_growth_rate'],
            bins=[-np.inf, -0.20, -0.05, 0.05, 0.20, np.inf],
            labels=['Declining', 'Soft', 'Stable', 'Growing', 'Hot']
        )

        self.logger.info("\nStyle Performance Distribution:")
        perf_dist = style_data['style_performance'].value_counts()
        for category, count in perf_dist.items():
            self.logger.info(f"  {category}: {count:,} store/styles ({count/len(style_data)*100:.1f}%)")

        
        # =========================================================================
        # STEP 3: CALCULATE SIZE MIX - RECENT MOMENTUM (TY 13 weeks)
        # =========================================================================
        self.logger.info("\nStep 3: Calculating recent momentum size mix (TY 13wk)...")
        
        # Total sales by store/style/size in TY 13 weeks
        size_sales_ty = df.groupby(['store_number', style_column, 'f5c_item_number']).agg({
            'pos_quantity_this_year': 'sum'
        }).reset_index()
        size_sales_ty.columns = ['store_number', style_column, 'f5c_item_number', 'size_sales_ty_13wk']
        
        # Total style sales for percentage calculation
        style_sales_ty = size_sales_ty.groupby(['store_number', style_column]).agg({
            'size_sales_ty_13wk': 'sum'
        }).reset_index()
        style_sales_ty.columns = ['store_number', style_column, 'style_total_sales_ty']
        
        size_sales_ty = size_sales_ty.merge(style_sales_ty, on=['store_number', style_column])

        self.logger.info(f"DEBUG: Columns after merge: {size_sales_ty.columns.tolist()}") # add

        # Calculate TY size mix percentage
        size_sales_ty['ty_size_mix_pct'] = np.where(
            size_sales_ty['style_total_sales_ty'] > 0,  # Use merged dataframe
            size_sales_ty['size_sales_ty_13wk'] / size_sales_ty['style_total_sales_ty'],  # Use merged dataframe
            0
        )
        
        self.logger.info(f"  Calculated TY size mix for {len(size_sales_ty):,} store/style/size combinations")
        
        # =========================================================================
        # STEP 4: CALCULATE SIZE MIX - HISTORICAL PATTERN (LY 13 weeks)
        # =========================================================================
        self.logger.info("\nStep 4: Calculating historical pattern size mix (LY 13wk)...")
        
        # Total sales by store/style/size in LY 13 weeks
        size_sales_ly = ly_df.groupby(['store_number', style_column, 'f5c_item_number']).agg({
            'pos_quantity_this_year': 'sum'  # Remember: this is actually LY data
        }).reset_index()
        size_sales_ly.columns = ['store_number', style_column, 'f5c_item_number', 'size_sales_ly_13wk']
        
        # Total style sales for percentage calculation
        style_sales_ly = size_sales_ly.groupby(['store_number', style_column]).agg({
            'size_sales_ly_13wk': 'sum'
        }).reset_index()
        style_sales_ly.columns = ['store_number', style_column, 'style_total_sales_ly']

        size_sales_ly = size_sales_ly.merge(style_sales_ly, on=['store_number', style_column])
        
        # Calculate LY size mix percentage
        size_sales_ly['ly_size_mix_pct'] = np.where(
            size_sales_ly['style_total_sales_ly'] > 0,  # Use merged dataframe
            size_sales_ly['size_sales_ly_13wk'] / size_sales_ly['style_total_sales_ly'],  # Use merged dataframe
            0
        )
        
        self.logger.info(f"  Calculated LY size mix for {len(size_sales_ly):,} store/style/size combinations")
        
        # =========================================================================
        # STEP 5: CALCULATE SIZE MIX - INVENTORY REALITY (Current Pipeline)
        # =========================================================================
        self.logger.info("\nStep 5: Calculating pipeline-based size mix (current inventory)...")
        
        # Calculate total pipeline by size
        current_week_df['total_pipeline'] = (
            current_week_df['on_hand_ty'] + 
            current_week_df['in_warehouse_ty'] + 
            current_week_df['in_transit_ty'] + 
            current_week_df['on_order_ty']
        )
        
        pipeline_mix = current_week_df.groupby(['store_number', style_column, 'f5c_item_number']).agg({
            'total_pipeline': 'sum'
        }).reset_index()
        
        # Total pipeline by style
        style_pipeline = pipeline_mix.groupby(['store_number', style_column]).agg({
            'total_pipeline': 'sum'
        }).reset_index()
        style_pipeline.columns = ['store_number', style_column, 'style_total_pipeline']
        
        # Calculate pipeline size mix
        pipeline_mix = pipeline_mix.merge(style_pipeline, on=['store_number', style_column])
        pipeline_mix['pipeline_size_mix_pct'] = np.where(
            pipeline_mix['style_total_pipeline'] > 0,
            pipeline_mix['total_pipeline'] / pipeline_mix['style_total_pipeline'],
            0
        )
        
        self.logger.info(f"  Calculated pipeline mix for {len(pipeline_mix):,} store/style/size combinations")
        
        # =========================================================================
        # STEP 6: ADAPTIVE BLENDING - Blend mixes based on style performance
        # =========================================================================
        self.logger.info("\nStep 6: Adaptive blending based on style performance...")
        
        # Merge all size mix data
        size_mix = size_sales_ty[['store_number', style_column, 'f5c_item_number', 
                                'ty_size_mix_pct', 'size_sales_ty_13wk']].copy()
        
        size_mix = size_mix.merge(
            size_sales_ly[['store_number', style_column, 'f5c_item_number', 
                        'ly_size_mix_pct', 'size_sales_ly_13wk']],
            on=['store_number', style_column, 'f5c_item_number'],
            how='outer'
        )
        
        size_mix = size_mix.merge(
            pipeline_mix[['store_number', style_column, 'f5c_item_number', 
                        'pipeline_size_mix_pct', 'total_pipeline']],
            on=['store_number', style_column, 'f5c_item_number'],
            how='outer'
        )
        
        # Merge style performance data
        size_mix = size_mix.merge(
            style_data[['store_number', style_column, 'style_growth_rate']],
            on=['store_number', style_column],
            how='left'
        )
        
        # Fill NaNs
        size_mix['ty_size_mix_pct'] = size_mix['ty_size_mix_pct'].fillna(0)
        size_mix['ly_size_mix_pct'] = size_mix['ly_size_mix_pct'].fillna(0)
        size_mix['pipeline_size_mix_pct'] = size_mix['pipeline_size_mix_pct'].fillna(0)
        size_mix['size_sales_ty_13wk'] = size_mix['size_sales_ty_13wk'].fillna(0)
        size_mix['size_sales_ly_13wk'] = size_mix['size_sales_ly_13wk'].fillna(0)
        size_mix['total_pipeline'] = size_mix['total_pipeline'].fillna(0)
        
        # ADAPTIVE BLENDING LOGIC
        # The weights change based on style performance:
        # - Hot/Growing styles: Follow recent momentum (high TY weight)
        # - Stable styles: Balance TY and LY equally
        # - Soft/Declining styles: Lean on LY pattern + inventory reality (save the style)
        
        def calculate_adaptive_weights(row):
            """
            Calculate blending weights based on style performance and data availability.
            
            Logic:
            - Hot styles (growth > 20%): 70% TY, 20% LY, 10% pipeline
            - Growing styles (5-20%): 60% TY, 25% LY, 15% pipeline
            - Stable styles (-5% to 5%): 45% TY, 35% LY, 20% pipeline
            - Soft styles (-20% to -5%): 30% TY, 40% LY, 30% pipeline
            - Declining styles (< -20%): 20% TY, 40% LY, 40% pipeline (inventory-driven)
            
            Additional logic:
            - If LY had more sales than TY for this size, increase LY weight
            - If no TY sales but has pipeline, increase pipeline weight
            """
            
            growth = row['style_growth_rate']
            ty_sales = row['size_sales_ty_13wk']
            ly_sales = row['size_sales_ly_13wk']
            pipeline = row['total_pipeline']
            
            # Base weights by performance
            if growth > 0.20:  # Hot
                w_ty, w_ly, w_pipeline = 0.70, 0.20, 0.10
            elif growth > 0.05:  # Growing
                w_ty, w_ly, w_pipeline = 0.60, 0.25, 0.15
            elif growth > -0.05:  # Stable
                w_ty, w_ly, w_pipeline = 0.45, 0.35, 0.20
            elif growth > -0.20:  # Soft
                w_ty, w_ly, w_pipeline = 0.30, 0.40, 0.30
            else:  # Declining
                w_ty, w_ly, w_pipeline = 0.20, 0.40, 0.40
            
            # Adjust if LY had stronger performance for this specific size
            if ly_sales > ty_sales and ty_sales > 0:
                ly_boost = min(0.15, (ly_sales - ty_sales) / ly_sales * 0.3)
                w_ly += ly_boost
                w_ty -= ly_boost * 0.7
                w_pipeline -= ly_boost * 0.3
            
            # Adjust if no TY sales but has pipeline (new sizes or stockouts)
            if ty_sales == 0 and pipeline > 0:
                if ly_sales > 0:
                    # Had LY sales, likely was out of stock TY
                    w_ly += 0.20
                    w_pipeline += 0.10
                    w_ty = max(0, w_ty - 0.30)
                else:
                    # New size, rely on pipeline
                    w_pipeline += 0.30
                    w_ty = max(0, w_ty - 0.15)
                    w_ly = max(0, w_ly - 0.15)
            
            # Normalize weights to sum to 1.0
            total = w_ty + w_ly + w_pipeline
            if total > 0:
                w_ty /= total
                w_ly /= total
                w_pipeline /= total
            
            return pd.Series({
                'weight_ty': w_ty,
                'weight_ly': w_ly,
                'weight_pipeline': w_pipeline
            })
        
        self.logger.info("  Calculating adaptive weights for each store/style/size...")
        weights = size_mix.apply(calculate_adaptive_weights, axis=1)
        size_mix = pd.concat([size_mix, weights], axis=1)
        
        # Calculate blended size mix
        size_mix['blended_size_mix_pct'] = (
            size_mix['weight_ty'] * size_mix['ty_size_mix_pct'] +
            size_mix['weight_ly'] * size_mix['ly_size_mix_pct'] +
            size_mix['weight_pipeline'] * size_mix['pipeline_size_mix_pct']
        )
        
        # Normalize blended mix to sum to 100% per store/style
        style_mix_totals = size_mix.groupby(['store_number', style_column]).agg({
            'blended_size_mix_pct': 'sum'
        }).reset_index()
        style_mix_totals.columns = ['store_number', style_column, 'mix_total']
        
        size_mix = size_mix.merge(style_mix_totals, on=['store_number', style_column])
        size_mix['blended_size_mix_pct'] = np.where(
            size_mix['mix_total'] > 0,
            size_mix['blended_size_mix_pct'] / size_mix['mix_total'],
            0
        )
        
        self.logger.info("\nBlending Weight Summary:")
        self.logger.info(f"  Avg TY weight: {size_mix['weight_ty'].mean():.1%}")
        self.logger.info(f"  Avg LY weight: {size_mix['weight_ly'].mean():.1%}")
        self.logger.info(f"  Avg Pipeline weight: {size_mix['weight_pipeline'].mean():.1%}")
        
        # =========================================================================
        # STEP 7: PROJECT STYLE-LEVEL DEMAND (Same as before)
        # =========================================================================
        self.logger.info("\nStep 7: Projecting style-level demand...")
        
        # This uses your existing logic - get LY actuals and apply growth
        # (You'll need to pass in ly_projection data here)
        # For now, using simple projection: LY avg * (1 + growth)

        self.logger.info(f"\nDEBUG - Before projection calculation:")
        self.logger.info(f"  Rows in style_data: {len(style_data)}")
        self.logger.info(f"  Unique styles: {style_data[style_column].nunique()}")
        self.logger.info(f"  Sample style_ly_13wk_avg: {style_data['style_ly_13wk_avg'].iloc[0]:.4f}")
        self.logger.info(f"  Sample style_growth_rate: {style_data['style_growth_rate'].iloc[0]:.4f}")
        
        style_data['style_projected_weekly_sales'] = style_data['style_ly_13wk_avg'] * (1 + style_data['style_growth_rate'])
        style_data['style_projected_weekly_sales'] = np.maximum(style_data['style_projected_weekly_sales'], 0.01)

        self.logger.info(f"\nDEBUG - After projection calculation:")
        self.logger.info(f"  Sample style_projected_weekly_sales: {style_data['style_projected_weekly_sales'].iloc[0]:.4f}")
        self.logger.info(f"  Total projected across all styles: {style_data['style_projected_weekly_sales'].sum():.2f}")
        
        # =========================================================================
        # STEP 8: DISTRIBUTE STYLE PROJECTION TO SIZES
        # =========================================================================
        self.logger.info("\nStep 8: Distributing style projections to size level...")
        
        # Drop any existing style-level columns from size_mix to avoid _x/_y suffixes
        cols_to_drop = ['style_projected_weekly_sales', 'style_ty_13wk_avg', 
                        'style_ly_13wk_avg', 'style_growth_rate']
        for col in cols_to_drop:
            if col in size_mix.columns:
                size_mix = size_mix.drop(columns=[col])

        # Merge style projection with size mix
        result = size_mix.merge(
            style_data[['store_number', style_column, 'style_projected_weekly_sales', 
                    'style_ty_13wk_avg', 'style_ly_13wk_avg', 'style_growth_rate']],
            on=['store_number', style_column],
            how='left'
        )

        # DEBUG: Check for duplication
        self.logger.info(f"\nDEBUG - After merging style projections:")
        self.logger.info(f"  Rows in size_mix: {len(size_mix)}")
        self.logger.info(f"  Rows in result: {len(result)}")
        self.logger.info(f"  Unique store/style/items in result: {result[['store_number', style_column, 'f5c_item_number']].drop_duplicates().shape[0]}")
        
        # Calculate size-level projection
        result['projected_weekly_sales'] = result['style_projected_weekly_sales'] * result['blended_size_mix_pct']

        # DEBUG: Check if calculation worked
        self.logger.info(f"\nDEBUG - After projection calculation:")
        self.logger.info(f"  Sample style_projected: {result['style_projected_weekly_sales'].iloc[0]:.4f}")
        self.logger.info(f"  Sample blended_mix: {result['blended_size_mix_pct'].iloc[0]:.4f}")
        self.logger.info(f"  Sample projected (should be above * above): {result['projected_weekly_sales'].iloc[0]:.4f}")
        self.logger.info(f"  Manual calc: {result['style_projected_weekly_sales'].iloc[0] * result['blended_size_mix_pct'].iloc[0]:.4f}")
        self.logger.info(f"  Projected sum: {result['projected_weekly_sales'].sum():.2f}")

        result['baseline_weekly_sales'] = result['size_sales_ty_13wk'] / 13
        
        # Ensure minimum projection
        result['projected_weekly_sales'] = np.maximum(result['projected_weekly_sales'], 0.01)
        
        self.logger.info(f"\n[OK] Style-based projection complete for {len(result):,} size-level records")
        self.logger.info(f"  Total projected weekly sales: {result['projected_weekly_sales'].sum():,.0f} units")
        if 'style_growth_rate' in result.columns:
            self.logger.info(f"  Average style growth rate: {result['style_growth_rate'].mean():.1%}")
        else:
            self.logger.info(f"  Average style growth rate: Not yet calculated")
                
        return result
