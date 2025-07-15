// Initialization for execution blocks visualization
document.addEventListener('DOMContentLoaded', function() {
    // Initialize execution blocks when the DOM is fully loaded
    initializeExecutionBlocks();
    
    // Make sure timestamps are formatted
    if (typeof formatTimestamps === 'function') {
        formatTimestamps();
    }
});

function initializeExecutionBlocks() {
    console.log('Initializing execution blocks visualization');
    
    // Set gas usage text for server-side rendered content
    $('.gas-info[data-gas-used]').each(function() {
        var gasUsed = parseInt($(this).data('gas-used')) || 0;
        var gasInM = (gasUsed / 1000000).toFixed(1) + 'M';
        $(this).text(gasInM);
    });
    
    // Position timeline dots for server-side rendered content
    $('.timeline-dot[data-position]').each(function() {
        var $dot = $(this);
        var position = parseInt($dot.data('position'));
        var topPos = position * 32 + 16;
        $dot.css('top', topPos + 'px');
    });
    
    // Set timeline dot colors based on gas usage
    $('.timeline-dot[data-gas-used]').each(function() {
        var gasUsed = parseInt($(this).data('gas-used')) || 0;
        var $this = $(this);
        $this.removeClass('gas-high gas-medium gas-low');
        if (gasUsed > 25000000) {
            $this.addClass('gas-high');
        } else if (gasUsed > 15000000) {
            $this.addClass('gas-medium');
        } else {
            $this.addClass('gas-low');
        }
    });
}
