def generate_demo_viz(result):
    """
    Generate a Pride & Prejudice themed visualization of the demo results.
    Call at the end of run_publish_demo(), passing the result dict.

    result dict should contain:
        safe_rows:  list of [r1, r2, r3, final] rows read during safe publish
        naive_rows: list of [r1, r2, r3, final] rows read during naive publish
                    use 0 to indicate a failed/error read
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.gridspec as gridspec
        from matplotlib.patches import FancyBboxPatch
    except ImportError:
        print("matplotlib not installed — skipping visualization")
        return

    # ── Palette ───────────────────────────────────────────────────────────
    PARCHMENT  = '#F5ECD7'
    INK        = '#2C1810'
    SEPIA      = '#8B6914'
    ROSE       = '#C4485A'
    SAGE       = '#5C7A4E'
    DUSTY_BLUE = '#4A6FA5'
    GOLD       = '#C9A84C'
    CREAM      = '#FAF4E8'

    plt.rcParams.update({
        'font.family':      'Georgia',
        'figure.facecolor': PARCHMENT,
        'axes.facecolor':   CREAM,
        'text.color':       INK,
        'axes.labelcolor':  INK,
        'xtick.color':      INK,
        'ytick.color':      INK,
        'axes.edgecolor':   SEPIA,
        'grid.color':       SEPIA,
        'grid.alpha':       0.3,
        'grid.linewidth':   0.8,
        'axes.grid':        True,
        'axes.spines.top':  False,
        'axes.spines.right':False,
    })

    # ── Data ──────────────────────────────────────────────────────────────
    times       = [0.0, 0.2, 1.2, 2.0, 3.8]
    safe_rows   = result.get("safe_rows",  [3500, 3500, 3500, 5000])
    naive_rows  = result.get("naive_rows", [0,    2000, 3000, 5000])

    safe_labels = [
        f'{r:,}\n(v1 complete)' if r == safe_rows[0] else f'{r:,}\n(v2 complete)'
        for r in safe_rows
    ]
    naive_labels = [
        'ERROR' if r == 0 else (f'{r:,}\n(partial!)' if r < naive_rows[-1] else f'{r:,}\n(v2 complete)')
        for r in naive_rows
    ]
    safe_colors  = [SAGE  if r < naive_rows[-1] else DUSTY_BLUE for r in safe_rows]
    naive_colors = [ROSE  if r < naive_rows[-1] else DUSTY_BLUE for r in naive_rows]

    # ── Layout ────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(16, 12), facecolor=PARCHMENT)
    gs  = gridspec.GridSpec(3, 2, figure=fig,
                             height_ratios=[0.12, 1, 0.55],
                             hspace=0.45, wspace=0.35,
                             left=0.07, right=0.95,
                             top=0.93, bottom=0.05)

    # Title
    ax_t = fig.add_subplot(gs[0, :])
    ax_t.set_xlim(0, 1); ax_t.set_ylim(0, 1); ax_t.axis('off')
    for sp in [0.02, 0.04]:
        ax_t.add_patch(FancyBboxPatch(
            (sp, sp), 1-2*sp, 1-2*sp,
            boxstyle="round,pad=0.01",
            linewidth=2 if sp == 0.04 else 0.8,
            edgecolor=GOLD, facecolor='none'))
    ax_t.text(0.5, 0.72, 'A Tale of Two Publishing Methods',
              ha='center', va='center', fontsize=22, color=INK,
              fontweight='bold', style='italic')
    ax_t.text(0.5, 0.28,
              'In which we demonstrate that manifests, like good manners, '
              'prevent considerable embarrassment',
              ha='center', va='center', fontsize=11, color=SEPIA, style='italic')

    # ── Timeline helper ───────────────────────────────────────────────────
    def draw_timeline(ax, rows, labels, colors, title, subtitle, title_color, show_manifest):
        ax.set_facecolor(CREAM)
        ax.set_xlim(-0.3, 4.5)
        ax.set_ylim(-400, 6400)

        ax.axvspan(0.0, 3.2, alpha=0.08,
                   color=SAGE if show_manifest else ROSE, zorder=0)
        ax.text(1.6, 6050, '<-- v2 uploading to staging -->',
                ha='center', fontsize=8, color=SEPIA, style='italic')

        if show_manifest:
            ax.axvline(x=3.2, color=SAGE, linewidth=2, linestyle='--', alpha=0.8)
            ax.text(3.25, 5400, 'manifest\nflips', fontsize=8, color=SAGE, va='top')

        v1 = safe_rows[0]
        v2 = safe_rows[-1]
        ax.axhline(y=v1, color=SEPIA,      linewidth=1, linestyle=':', alpha=0.6)
        ax.axhline(y=v2, color=DUSTY_BLUE, linewidth=1, linestyle=':', alpha=0.6)
        ax.text(4.4, v1, 'v1', fontsize=8, color=SEPIA,      va='center')
        ax.text(4.4, v2, 'v2', fontsize=8, color=DUSTY_BLUE, va='center')

        valid_t = [t for t, r in zip(times, rows) if r > 0]
        valid_r = [r for r in rows if r > 0]
        if len(valid_t) > 1:
            ax.plot(valid_t, valid_r, color=colors[-1], linewidth=1.5, alpha=0.4)

        for i, (t, r, lbl, c) in enumerate(zip(times, rows, labels, colors)):
            ax.axvline(x=t, color=SEPIA, linewidth=0.5, linestyle=':', alpha=0.4)
            if r == 0:
                ax.scatter([t], [300], s=200, color=ROSE, zorder=5,
                           marker='X', linewidths=2)
                ax.text(t, 650, lbl, ha='center', va='bottom',
                        fontsize=9, color=ROSE, fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor=CREAM,
                                  edgecolor=ROSE, linewidth=1.5))
            else:
                ax.scatter([t], [r], s=150, color=c, zorder=5,
                           edgecolors=INK, linewidths=0.8)
                offset = 400 if i % 2 == 0 else -600
                ax.annotate(lbl, xy=(t, r), xytext=(t, r + offset),
                            ha='center', va='center', fontsize=8.5, color=INK,
                            bbox=dict(boxstyle='round,pad=0.3', facecolor=CREAM,
                                      edgecolor=c, linewidth=1.5),
                            arrowprops=dict(arrowstyle='->', color=c, lw=1.2))

        ax.set_xlabel('Time elapsed (seconds)', fontsize=10)
        ax.set_ylabel('Rows read by consumer', fontsize=10)
        ax.set_title(title, fontsize=13, fontweight='bold', color=title_color, pad=18)
        ax.text(
            0.5,
            0.985,
            subtitle,
            transform=ax.transAxes,
            ha='center',
            va='top',
            fontsize=8.5,
            color=SEPIA,
            style='italic'
        )
        ax.set_xticks([0.0, 0.2, 1.2, 2.0, 3.2, 3.8])
        ax.set_xticklabels(
            [
                't=0.0s\n(baseline)',
                't=0.2s\n(reader 1)',
                't=1.2s\n(reader 2)',
                't=2.0s\n(reader 3)',
                'upload\ncomplete',
                't=3.8s\n(final)'
            ],
            fontsize=8
        )
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f'{int(x):,}'))

    ax_safe  = fig.add_subplot(gs[1, 0])
    ax_naive = fig.add_subplot(gs[1, 1])

    draw_timeline(ax_safe, safe_rows, safe_labels, safe_colors,
                  'The Manifested Gentleman',
                  '"I shall not be precipitate -- the manifest ensures consistency"',
                  SAGE, True)
    draw_timeline(ax_naive, naive_rows, naive_labels, naive_colors,
                  'The Impetuous Overwriter',
                  '"He deleted first and uploaded later -- a most imprudent arrangement"',
                  ROSE, False)

    # ── Cost panel ────────────────────────────────────────────────────────
    ax_cost = fig.add_subplot(gs[2, :])
    ax_cost.axis('off')
    ax_cost.add_patch(FancyBboxPatch(
        (0.01, 0.04), 0.98, 0.92,
        boxstyle="round,pad=0.01", linewidth=1.5,
        edgecolor=GOLD, facecolor=CREAM,
        transform=ax_cost.transAxes))

    ax_cost.text(0.5, 0.88, 'The Ledger of Consequences',
                 ha='center', va='center', fontsize=13, fontweight='bold',
                 color=INK, style='italic', transform=ax_cost.transAxes)

    naive_items = [
        ('[X]', 'Failed read at t=0.2s',
                'Pipeline crash -- downstream jobs receive nothing'),
        ('[!]', 'Partial read at t=1.2s',
                f'{naive_rows[1]:,} of {naive_rows[-1]:,} rows -- silent data corruption'),
        ('[!]', 'Partial read at t=2.0s',
                f'{naive_rows[2]:,} of {naive_rows[-1]:,} rows -- aggregations are wrong'),
        ('[$]', 'Engineer investigation (2h @ 150/h)',
                '300 GBP per incident'),
        ('[%]', 'Occurring 100x/day in production',
                '30,000 GBP/day in wasted engineering time'),
    ]
    safe_items = [
        ('[v]', 'Validation overhead',     'S: 2.3s   M: 51s   L: 15s'),
        ('[v]', 'Manifest write overhead', 'Always < 1 second'),
        ('[v]', 'Total publish overhead',  '3-11% above upload time'),
        ('[v]', 'Corrupted reads',         'Zero. Guaranteed.'),
        ('[v]', 'Rollback capability',     'previous.json always preserved'),
    ]

    ax_cost.text(0.01, 0.76, 'Cost of Naive Publishing',
                 ha='left', fontsize=10, fontweight='bold',
                 color=ROSE, transform=ax_cost.transAxes)
    for i, (icon, item, desc) in enumerate(naive_items):
        y = 0.65 - i * 0.115
        ax_cost.text(0.02, y, f'{icon}  {item}',
                     ha='left', fontsize=9, color=INK,
                     transform=ax_cost.transAxes)
        ax_cost.text(0.02, y - 0.052, f'      -> {desc}',
                     ha='left', fontsize=8.5, color=ROSE, style='italic',
                     transform=ax_cost.transAxes)

    ax_cost.text(0.51, 0.76, 'Cost of Manifest-Based Publishing',
                 ha='left', fontsize=10, fontweight='bold',
                 color=SAGE, transform=ax_cost.transAxes)
    for i, (icon, item, desc) in enumerate(safe_items):
        y = 0.65 - i * 0.115
        ax_cost.text(0.52, y, f'{icon}  {item}',
                     ha='left', fontsize=9, color=INK,
                     transform=ax_cost.transAxes)
        ax_cost.text(0.52, y - 0.052, f'      -> {desc}',
                     ha='left', fontsize=8.5, color=SAGE, style='italic',
                     transform=ax_cost.transAxes)

    ax_cost.plot([0.5, 0.5], [0.08, 0.92],
                 color=GOLD, linewidth=1, transform=ax_cost.transAxes)
    ax_cost.text(0.5, 0.08,
                 '"It is a truth universally acknowledged that a pipeline '
                 'in possession of good data must be in want of a manifest."',
                 ha='center', va='center', fontsize=9.5,
                 color=SEPIA, style='italic', transform=ax_cost.transAxes)

    plt.savefig('demo_results.png', dpi=150, bbox_inches='tight',
                facecolor=PARCHMENT)
    plt.close()
    print("\n📊 Visualization saved to demo_results.png")