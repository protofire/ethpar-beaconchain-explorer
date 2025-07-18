// Initialization for execution blocks visualization with dropdown
document.addEventListener('DOMContentLoaded', function() {
    // Initialize execution blocks when the DOM is fully loaded
    initializeExecutionBlocks();
    
    // Make sure timestamps are formatted
    if (typeof formatTimestamps === 'function') {
        formatTimestamps();
    }
});

function initializeExecutionBlocks() {
    console.log('Initializing execution blocks visualization with dropdown');
    
    // Set gas usage text for server-side rendered content
    $('.gas-info[data-gas-used]').each(function() {
        var gasUsed = parseInt($(this).data('gas-used')) || 0;
        var gasInM = (gasUsed / 1000000).toFixed(1) + 'M';
        $(this).text(gasInM);
    });
    
    // Set additional count for dropdown buttons
    $('.dropdown-toggle-btn[data-total-ranks]').each(function() {
        var totalRanks = parseInt($(this).data('total-ranks')) || 0;
        var additionalCount = Math.max(0, totalRanks - 1);
        $(this).find('.additional-count').text('+' + additionalCount);
    });
    
    // Initialize Bootstrap dropdowns for execution blocks
    $('.execution-ranks-dropdown .dropdown-toggle-btn').on('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        
        var $dropdown = $(this).siblings('.dropdown-menu');
        var $allDropdowns = $('.execution-ranks-dropdown .dropdown-menu');
        
        // Close all other dropdowns
        $allDropdowns.not($dropdown).removeClass('show');
        
        // Toggle current dropdown
        $dropdown.toggleClass('show');
    });
    
    // Close dropdowns when clicking outside
    $(document).on('click', function(e) {
        if (!$(e.target).closest('.execution-ranks-dropdown').length) {
            $('.execution-ranks-dropdown .dropdown-menu').removeClass('show');
        }
    });
    
    // Handle dropdown item clicks
    $('.dropdown-menu .execution-block-item').on('click', function(e) {
        e.stopPropagation();
        // Let the link work normally, but close the dropdown
        $(this).closest('.dropdown-menu').removeClass('show');
    });
}
